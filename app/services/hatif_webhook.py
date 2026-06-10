"""Hatif / Voxa inbound webhook parsing and persistence."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from fastapi import HTTPException
from sqlalchemy import and_, desc, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.models import MessageLog, WebhookEvent

HATIF_STATUS_EVENT_PREFIX = "HatifStatus:"
HATIF_CALL_EVENT_PREFIX = "HatifCall:"
HATIF_WEBHOOK_EVENT_PREFIX = "Hatif"
from app.utils.signature import verify_voxa_signature

logger = logging.getLogger("app.hatif_webhook")

SUCCESS_STATUSES = frozenset({"sent", "delivered", "read", "success", "pending"})
FAILED_STATUSES = frozenset({"failed"})

CALL_STATUS_LABELS: dict[int, str] = {
    0: "Active",
    1: "Completed",
    2: "Missed",
    3: "RejectedByCaller",
    4: "RejectedByCallee",
    5: "NoAnswer",
    6: "Cancelled",
    7: "Failed",
    8: "Ringing",
}

CALL_DIRECTION_LABELS: dict[int, str] = {
    1: "Inbound",
    2: "Outbound",
}


def _ci(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
        lower = key.lower()
        for k, v in payload.items():
            if k.lower() == lower:
                return v
    return None


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        return None


def verify_hatif_webhook(body_utf8: str, signature: str | None, request_id: str) -> None:
    """Raise 401 if secret is configured and signature is missing or invalid."""
    if not settings.HATIF_WEBHOOK_SECRET:
        logger.debug(
            "hatif_webhook_signature_skipped_no_secret",
            extra={"extra": {"request_id": request_id}},
        )
        return

    if not verify_voxa_signature(body_utf8, settings.HATIF_WEBHOOK_SECRET, signature):
        logger.warning(
            "hatif_webhook_signature_invalid",
            extra={
                "extra": {
                    "request_id": request_id,
                    "signature_received": (
                        signature[:20] + "..."
                        if signature and len(signature) > 20
                        else signature
                    ),
                }
            },
        )
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.debug(
        "hatif_webhook_signature_valid",
        extra={"extra": {"request_id": request_id}},
    )


def parse_json_body(body_utf8: str, request_id: str) -> dict[str, Any]:
    try:
        payload = json.loads(body_utf8)
        if not isinstance(payload, dict):
            return {}
        return payload
    except Exception:
        logger.warning(
            "hatif_webhook_invalid_json",
            extra={"extra": {"request_id": request_id}},
            exc_info=True,
        )
        return {}


@dataclass
class WhatsAppWebhookPayload:
    workspace_id: str | None
    channel_id: str | None
    conversation_id: str | None
    contact_id: str | None
    message_id: str | None
    direction: str | None
    message_type: str | None
    body: str | None
    status: str | None
    creation_time: datetime | None
    is_billable: bool | None
    error_code: int | None
    error_reason: str | None

    @property
    def status_normalized(self) -> str:
        return str(self.status or "").strip().lower()


def parse_whatsapp_payload(raw: dict[str, Any]) -> WhatsAppWebhookPayload:
    error_code = _ci(raw, "errorCode")
    if error_code is not None:
        try:
            error_code = int(error_code)
        except (TypeError, ValueError):
            error_code = None

    billable = _ci(raw, "isBillable")
    if billable is not None:
        billable = bool(billable)

    return WhatsAppWebhookPayload(
        workspace_id=_as_str(_ci(raw, "workspaceId", "workspaceID")),
        channel_id=_as_str(_ci(raw, "channelId", "channelID")),
        conversation_id=_as_str(_ci(raw, "conversationId", "conversationID")),
        contact_id=_as_str(_ci(raw, "contactId", "contactID")),
        message_id=_as_str(_ci(raw, "messageId", "messageID")),
        direction=_as_str(_ci(raw, "direction")),
        message_type=_as_str(_ci(raw, "messageType")),
        body=_as_str(_ci(raw, "body")),
        status=_as_str(_ci(raw, "status")),
        creation_time=parse_datetime(_ci(raw, "creationTime", "timestamp")),
        is_billable=billable,
        error_code=error_code,
        error_reason=_as_str(_ci(raw, "errorReason")),
    )


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _channel_matches(payload_channel: str | None) -> bool:
    expected = (settings.HATIF_CHANNEL_ID or "").strip()
    if not expected or not payload_channel:
        return True
    return payload_channel.lower() == expected.lower()


def find_message_log(db: Session, payload: WhatsAppWebhookPayload, request_id: str) -> MessageLog | None:
    if payload.message_id:
        matched = db.execute(
            select(MessageLog).where(MessageLog.message_id == payload.message_id)
        ).scalar_one_or_none()
        if matched:
            logger.info(
                "hatif_webhook_matched_by_message_id",
                extra={"extra": {"request_id": request_id, "message_log_id": matched.id}},
            )
            return matched

    correlation_ids: list[str] = []
    if payload.conversation_id:
        correlation_ids.append(payload.conversation_id)

    for cid in correlation_ids:
        matched = db.execute(
            select(MessageLog).where(MessageLog.conversation_event_id == cid)
        ).scalar_one_or_none()
        if matched:
            logger.info(
                "hatif_webhook_matched_by_conversation_id",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "message_log_id": matched.id,
                        "conversation_id": cid,
                    }
                },
            )
            return matched

    if payload.contact_id and payload.channel_id:
        since = datetime.utcnow() - timedelta(hours=48)
        matched = db.execute(
            select(MessageLog)
            .where(
                and_(
                    MessageLog.contact_id == payload.contact_id,
                    MessageLog.channel_id == payload.channel_id,
                    MessageLog.created_at >= since,
                    or_(
                        MessageLog.direction.is_(None),
                        MessageLog.direction.ilike("outbound"),
                    ),
                )
            )
            .order_by(desc(MessageLog.created_at))
        ).scalars().first()
        if matched:
            logger.info(
                "hatif_webhook_matched_by_contact_channel_fallback",
                extra={"extra": {"request_id": request_id, "message_log_id": matched.id}},
            )
            return matched

    return None


def find_message_log_with_legacy(
    db: Session,
    payload: WhatsAppWebhookPayload,
    legacy_conversation_event_id: str | None,
    request_id: str,
) -> MessageLog | None:
    if legacy_conversation_event_id:
        matched = db.execute(
            select(MessageLog).where(
                MessageLog.conversation_event_id == legacy_conversation_event_id
            )
        ).scalar_one_or_none()
        if matched:
            logger.info(
                "hatif_webhook_matched_by_conversation_event_id",
                extra={"extra": {"request_id": request_id, "message_log_id": matched.id}},
            )
            return matched
    return find_message_log(db, payload, request_id)


def apply_whatsapp_status_update(
    db: Session,
    matched: MessageLog | None,
    payload: WhatsAppWebhookPayload,
    raw: dict[str, Any],
    request_id: str,
) -> MessageLog:
    provider_response = json.dumps(raw, ensure_ascii=False)
    status_norm = payload.status_normalized

    if matched:
        old_status = matched.last_status
        matched.conversation_event_id = (
            legacy_conversation_event_id(raw)
            or payload.conversation_id
            or matched.conversation_event_id
        )
        matched.contact_id = payload.contact_id or matched.contact_id
        matched.channel_id = payload.channel_id or matched.channel_id
        matched.last_status = payload.status or matched.last_status
        matched.last_status_at = payload.creation_time or matched.last_status_at
        matched.direction = payload.direction or matched.direction
        matched.message_id = payload.message_id or matched.message_id
        matched.error_code = payload.error_code
        matched.error_reason = payload.error_reason
        matched.provider_response = provider_response
        if status_norm in FAILED_STATUSES:
            matched.status = "failed"
        elif status_norm in SUCCESS_STATUSES and matched.status == "failed":
            matched.status = "success"
        db.add(matched)
        db.commit()
        logger.info(
            "hatif_webhook_message_log_updated",
            extra={
                "extra": {
                    "request_id": request_id,
                    "message_log_id": matched.id,
                    "old_status": old_status,
                    "new_status": payload.status,
                    "direction": payload.direction,
                    "message_type": payload.message_type,
                    "error_code": payload.error_code,
                }
            },
        )
        return matched

    log = MessageLog(
        phone=None,
        template_name=payload.message_type if payload.message_type == "Template" else None,
        status="success" if status_norm in SUCCESS_STATUSES else "failed",
        provider_response=provider_response,
        conversation_event_id=legacy_conversation_event_id(raw) or payload.conversation_id,
        contact_id=payload.contact_id,
        channel_id=payload.channel_id,
        last_status=payload.status,
        last_status_at=payload.creation_time,
        direction=payload.direction,
        message_id=payload.message_id,
        error_code=payload.error_code,
        error_reason=payload.error_reason,
    )
    db.add(log)
    db.commit()
    logger.info(
        "hatif_webhook_message_log_inserted_fallback",
        extra={
            "extra": {
                "request_id": request_id,
                "message_log_id": log.id,
                "status": payload.status,
                "direction": payload.direction,
                "message_type": payload.message_type,
            }
        },
    )
    return log


def record_hatif_status_activity(
    db: Session,
    payload: WhatsAppWebhookPayload,
    raw: dict[str, Any],
    message_log: MessageLog,
    request_id: str,
) -> None:
    """Persist one activity-log row per inbound Hatif delivery webhook."""
    status = (payload.status or "Unknown").strip()
    ts_key = (
        payload.creation_time.isoformat()
        if payload.creation_time
        else datetime.utcnow().isoformat()
    )
    msg_key = payload.message_id or payload.conversation_id or message_log.id
    external_id = f"hatif-wa:{msg_key}:{status}:{ts_key}"

    activity_payload = {
        **raw,
        "_sumo": {
            "message_log_id": message_log.id,
            "request_id": request_id,
            "phone": message_log.phone,
            "template_name": message_log.template_name,
            "delivery_status": status,
        },
    }

    row = WebhookEvent(
        external_event_id=external_id,
        event_name=f"{HATIF_STATUS_EVENT_PREFIX}{status}",
        phone=message_log.phone,
        payload_json=json.dumps(activity_payload, ensure_ascii=False),
    )
    try:
        db.add(row)
        db.commit()
        logger.info(
            "hatif_status_activity_recorded",
            extra={
                "extra": {
                    "request_id": request_id,
                    "webhook_event_id": row.id,
                    "message_log_id": message_log.id,
                    "delivery_status": status,
                }
            },
        )
    except IntegrityError:
        db.rollback()
        logger.info(
            "hatif_status_activity_duplicate",
            extra={
                "extra": {
                    "request_id": request_id,
                    "external_event_id": external_id,
                }
            },
        )


def legacy_conversation_event_id(raw: dict[str, Any]) -> str | None:
    return _as_str(_ci(raw, "conversationEventId", "conversationEventID"))


def process_whatsapp_webhook(db: Session, body_utf8: str, request_id: str) -> dict[str, str]:
    raw = parse_json_body(body_utf8, request_id)
    payload = parse_whatsapp_payload(raw)

    if not _channel_matches(payload.channel_id):
        logger.warning(
            "hatif_webhook_channel_mismatch",
            extra={
                "extra": {
                    "request_id": request_id,
                    "payload_channel_id": payload.channel_id,
                    "expected_channel_id": settings.HATIF_CHANNEL_ID,
                }
            },
        )

    logger.info(
        "hatif_webhook_fields_extracted",
        extra={
            "extra": {
                "request_id": request_id,
                "workspace_id": payload.workspace_id,
                "channel_id": payload.channel_id,
                "conversation_id": payload.conversation_id,
                "contact_id": payload.contact_id,
                "message_id": payload.message_id,
                "direction": payload.direction,
                "message_type": payload.message_type,
                "status": payload.status,
                "error_code": payload.error_code,
                "error_reason": payload.error_reason,
            }
        },
    )

    matched = find_message_log_with_legacy(
        db,
        payload,
        legacy_conversation_event_id(raw),
        request_id,
    )
    if not matched:
        logger.info(
            "hatif_webhook_no_match_found",
            extra={
                "extra": {
                    "request_id": request_id,
                    "conversation_id": payload.conversation_id,
                    "contact_id": payload.contact_id,
                    "channel_id": payload.channel_id,
                    "message_id": payload.message_id,
                }
            },
        )

    message_log = apply_whatsapp_status_update(db, matched, payload, raw, request_id)
    record_hatif_status_activity(db, payload, raw, message_log, request_id)
    return {"status": "ok"}


@dataclass
class CallWebhookPayload:
    call_id: str
    workspace_id: str | None
    channel_id: str | None
    status: int | None
    direction: int | None
    caller_number: str | None
    callee_number: str | None
    contact_id: str | None
    contact_number: str | None
    call_length: str | None
    creation_time: datetime | None
    recording_url: str | None
    summary: str | None


def parse_call_payload(raw: dict[str, Any]) -> CallWebhookPayload | None:
    call_id = _as_str(_ci(raw, "callId", "callID"))
    if not call_id:
        return None

    status = _ci(raw, "status")
    if status is not None:
        try:
            status = int(status)
        except (TypeError, ValueError):
            status = None

    direction = _ci(raw, "type")
    if direction is not None:
        try:
            direction = int(direction)
        except (TypeError, ValueError):
            direction = None

    return CallWebhookPayload(
        call_id=call_id,
        workspace_id=_as_str(_ci(raw, "workspaceId", "workspaceID")),
        channel_id=_as_str(_ci(raw, "channelId", "channelID")),
        status=status,
        direction=direction,
        caller_number=_as_str(_ci(raw, "callerNumber")),
        callee_number=_as_str(_ci(raw, "calleeNumber")),
        contact_id=_as_str(_ci(raw, "contactId", "contactID")),
        contact_number=_as_str(_ci(raw, "contactNumber")),
        call_length=_as_str(_ci(raw, "callLength")),
        creation_time=parse_datetime(_ci(raw, "creationTime")),
        recording_url=_as_str(_ci(raw, "recordingUrl")),
        summary=_as_str(_ci(raw, "summary")),
    )


def process_call_webhook(db: Session, body_utf8: str, request_id: str) -> dict[str, str]:
    raw = parse_json_body(body_utf8, request_id)
    payload = parse_call_payload(raw)
    if not payload:
        logger.warning(
            "hatif_call_webhook_missing_call_id",
            extra={"extra": {"request_id": request_id}},
        )
        return {"status": "ok"}

    if not _channel_matches(payload.channel_id):
        logger.warning(
            "hatif_call_webhook_channel_mismatch",
            extra={
                "extra": {
                    "request_id": request_id,
                    "payload_channel_id": payload.channel_id,
                    "expected_channel_id": settings.HATIF_CHANNEL_ID,
                }
            },
        )

    status_label = CALL_STATUS_LABELS.get(payload.status or -1, str(payload.status))
    direction_label = CALL_DIRECTION_LABELS.get(payload.direction or -1, str(payload.direction))
    phone = payload.contact_number or payload.callee_number or payload.caller_number

    logger.info(
        "hatif_call_webhook_fields_extracted",
        extra={
            "extra": {
                "request_id": request_id,
                "call_id": payload.call_id,
                "status": payload.status,
                "status_label": status_label,
                "direction": direction_label,
                "contact_number": payload.contact_number,
                "call_length": payload.call_length,
            }
        },
    )

    event_name = f"HatifCall:{status_label}"
    payload_json = json.dumps(raw, ensure_ascii=False)

    existing = db.execute(
        select(WebhookEvent).where(WebhookEvent.external_event_id == payload.call_id)
    ).scalar_one_or_none()

    if existing:
        existing.event_name = event_name
        existing.phone = phone
        existing.payload_json = payload_json
        db.add(existing)
        db.commit()
        logger.info(
            "hatif_call_webhook_updated",
            extra={
                "extra": {
                    "request_id": request_id,
                    "call_id": payload.call_id,
                    "webhook_event_id": existing.id,
                }
            },
        )
    else:
        row = WebhookEvent(
            external_event_id=payload.call_id,
            event_name=event_name,
            phone=phone,
            payload_json=payload_json,
        )
        db.add(row)
        db.commit()
        logger.info(
            "hatif_call_webhook_saved",
            extra={
                "extra": {
                    "request_id": request_id,
                    "call_id": payload.call_id,
                    "webhook_event_id": row.id,
                }
            },
        )

    return {"status": "ok"}


def hatif_webhook_urls(public_base: str) -> dict[str, str]:
    base = public_base.rstrip("/")
    return {
        "whatsapp": f"{base}/webhooks/hatif/whatsapp",
        "call": f"{base}/webhooks/hatif/call",
    }
