import json
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import and_, desc, select
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import MessageLog
from app.utils.signature import verify_voxa_signature

router = APIRouter()
logger = logging.getLogger("app.hatif_webhook")
SUCCESS_STATUSES = {"sent", "delivered", "read", "success"}


def _ci(payload: dict, *keys: str):
    """Case-insensitive key lookup: try each key as-is, then lower-cased."""
    for key in keys:
        if key in payload:
            return payload[key]
        lower = key.lower()
        for k, v in payload.items():
            if k.lower() == lower:
                return v
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        logger.debug("datetime_parse_failed", extra={"extra": {"value": value}})
        return None


@router.post("/webhooks/hatif/whatsapp")
async def hatif_whatsapp_webhook(
    request: Request,
    db: Session = Depends(get_db),
    signature: str | None = Header(default=None, alias="X-Voxa-Signature"),
):
    body = await request.body()
    body_utf8 = body.decode("utf-8", errors="replace")
    request_id = request.state.request_id

    logger.info(
        "hatif_webhook_received",
        extra={
            "extra": {
                "request_id": request_id,
                "body_size": len(body),
                "has_signature": signature is not None,
            }
        },
    )

    # Signature verification: OPTIONAL if secret is empty
    if settings.HATIF_WEBHOOK_SECRET:
        logger.debug(
            "hatif_webhook_verifying_signature",
            extra={"extra": {"request_id": request_id}},
        )
        if not verify_voxa_signature(body_utf8, settings.HATIF_WEBHOOK_SECRET, signature):
            logger.warning(
                "hatif_webhook_signature_invalid",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "signature_received": signature[:20] + "..." if signature and len(signature) > 20 else signature,
                    }
                },
            )
            raise HTTPException(status_code=401, detail="Unauthorized")
        logger.debug(
            "hatif_webhook_signature_valid",
            extra={"extra": {"request_id": request_id}},
        )
    else:
        logger.debug(
            "hatif_webhook_signature_skipped_no_secret",
            extra={"extra": {"request_id": request_id}},
        )

    try:
        payload = json.loads(body_utf8)
        logger.info(
            "hatif_webhook_payload_parsed",
            extra={
                "extra": {
                    "request_id": request_id,
                    "payload_keys": list(payload.keys()) if isinstance(payload, dict) else "not_dict",
                }
            },
        )
    except Exception:
        logger.warning(
            "hatif_webhook_invalid_json",
            extra={"extra": {"request_id": request_id}},
            exc_info=True,
        )
        payload = {}

    conversation_event_id = _ci(payload, "conversationEventId", "conversationEventID")
    contact_id = _ci(payload, "contactId", "contactID")
    channel_id = _ci(payload, "channelId", "channelID")
    message_id = _ci(payload, "messageId", "messageID")
    direction = _ci(payload, "direction")
    status = _ci(payload, "status")
    status_value = str(status).lower() if status is not None else ""
    status_at = _parse_datetime(_ci(payload, "timestamp", "creationTime"))
    error_code = _ci(payload, "errorCode")
    error_reason = _ci(payload, "errorReason")

    logger.info(
        "hatif_webhook_fields_extracted",
        extra={
            "extra": {
                "request_id": request_id,
                "conversation_event_id": conversation_event_id,
                "contact_id": contact_id,
                "channel_id": channel_id,
                "message_id": message_id,
                "direction": direction,
                "status": status,
                "error_code": error_code,
                "error_reason": error_reason,
            }
        },
    )

    # --- Correlation: find existing MessageLog ---
    matched = None

    if conversation_event_id:
        logger.debug(
            "hatif_webhook_searching_by_conversation_event_id",
            extra={"extra": {"request_id": request_id, "conversation_event_id": conversation_event_id}},
        )
        matched = db.execute(
            select(MessageLog).where(
                MessageLog.conversation_event_id == conversation_event_id
            )
        ).scalar_one_or_none()
        if matched:
            logger.info(
                "hatif_webhook_matched_by_conversation_event_id",
                extra={"extra": {"request_id": request_id, "message_log_id": matched.id}},
            )

    if not matched and contact_id and channel_id:
        since = datetime.utcnow() - timedelta(hours=24)
        logger.debug(
            "hatif_webhook_searching_by_contact_channel_fallback",
            extra={
                "extra": {
                    "request_id": request_id,
                    "contact_id": contact_id,
                    "channel_id": channel_id,
                    "since": since.isoformat(),
                }
            },
        )
        matched = db.execute(
            select(MessageLog)
            .where(
                and_(
                    MessageLog.contact_id == contact_id,
                    MessageLog.channel_id == channel_id,
                    MessageLog.created_at >= since,
                )
            )
            .order_by(desc(MessageLog.created_at))
        ).scalars().first()
        if matched:
            logger.info(
                "hatif_webhook_matched_by_contact_channel_fallback",
                extra={"extra": {"request_id": request_id, "message_log_id": matched.id}},
            )

    if not matched:
        logger.info(
            "hatif_webhook_no_match_found",
            extra={
                "extra": {
                    "request_id": request_id,
                    "conversation_event_id": conversation_event_id,
                    "contact_id": contact_id,
                    "channel_id": channel_id,
                }
            },
        )

    provider_response = json.dumps(payload)

    if matched:
        old_status = matched.last_status
        matched.conversation_event_id = conversation_event_id or matched.conversation_event_id
        matched.contact_id = contact_id or matched.contact_id
        matched.channel_id = channel_id or matched.channel_id
        matched.last_status = status or matched.last_status
        matched.last_status_at = status_at or matched.last_status_at
        matched.direction = direction or matched.direction
        matched.message_id = message_id or matched.message_id
        matched.error_code = error_code
        matched.error_reason = error_reason
        matched.provider_response = provider_response
        db.add(matched)
        db.commit()
        logger.info(
            "hatif_webhook_message_log_updated",
            extra={
                "extra": {
                    "request_id": request_id,
                    "message_log_id": matched.id,
                    "old_status": old_status,
                    "new_status": status,
                    "direction": direction,
                    "error_code": error_code,
                }
            },
        )
        return {"status": "ok"}

    # Fallback: insert new row
    log = MessageLog(
        phone=None,
        template_name=None,
        status="success" if status_value in SUCCESS_STATUSES else "failed",
        provider_response=provider_response,
        conversation_event_id=conversation_event_id,
        contact_id=contact_id,
        channel_id=channel_id,
        last_status=status,
        last_status_at=status_at,
        direction=direction,
        message_id=message_id,
        error_code=error_code,
        error_reason=error_reason,
    )
    db.add(log)
    db.commit()
    logger.info(
        "hatif_webhook_message_log_inserted_fallback",
        extra={
            "extra": {
                "request_id": request_id,
                "message_log_id": log.id,
                "status": status,
                "direction": direction,
            }
        },
    )

    return {"status": "ok"}
