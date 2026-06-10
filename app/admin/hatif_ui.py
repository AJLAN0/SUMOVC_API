"""Dashboard helpers for Hatif / Voxa inbound webhooks."""

from __future__ import annotations

import json
from typing import Any

from app.services.hatif_webhook import (
    CALL_DIRECTION_LABELS,
    CALL_STATUS_LABELS,
    HATIF_CALL_EVENT_PREFIX,
    HATIF_STATUS_EVENT_PREFIX,
    HATIF_WEBHOOK_EVENT_PREFIX,
)

HATIF_KIND_LABELS_AR: dict[str, str] = {
    "whatsapp_status": "تسليم واتساب",
    "call": "مكالمة",
    "unknown": "غير معروف",
}

HATIF_KIND_ORDER: list[str] = ["whatsapp_status", "call"]

HATIF_DELIVERY_STATUS_LABELS_AR: dict[str, str] = {
    "sent": "تم الإرسال",
    "delivered": "تم التسليم",
    "read": "تمت القراءة",
    "pending": "قيد الانتظار",
    "failed": "فشل التسليم",
}


def is_hatif_webhook(event_name: str | None) -> bool:
    return bool(event_name and event_name.startswith(HATIF_WEBHOOK_EVENT_PREFIX))


def hatif_webhook_kind(event_name: str | None) -> str:
    if not event_name:
        return "unknown"
    if event_name.startswith(HATIF_STATUS_EVENT_PREFIX):
        return "whatsapp_status"
    if event_name.startswith(HATIF_CALL_EVENT_PREFIX):
        return "call"
    return "unknown"


def hatif_kind_label(kind: str) -> str:
    return HATIF_KIND_LABELS_AR.get(kind, kind)


def _parse_payload(payload_json: str) -> dict[str, Any]:
    try:
        data = json.loads(payload_json)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _ci(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
        lower = key.lower()
        for k, v in payload.items():
            if k.lower() == lower:
                return v
    return None


def delivery_status_from_event(event_name: str | None) -> str | None:
    if not event_name or not event_name.startswith(HATIF_STATUS_EVENT_PREFIX):
        return None
    return event_name.replace(HATIF_STATUS_EVENT_PREFIX, "", 1)


def delivery_status_label(status: str | None) -> str:
    if not status:
        return "—"
    return HATIF_DELIVERY_STATUS_LABELS_AR.get(status.lower(), status)


def call_status_label(status: int | None) -> str:
    if status is None:
        return "—"
    return CALL_STATUS_LABELS.get(status, str(status))


def call_direction_label(direction: int | None) -> str:
    if direction is None:
        return "—"
    return CALL_DIRECTION_LABELS.get(direction, str(direction))


def hatif_event_row_context(event: Any) -> dict[str, Any]:
    """Build list-row context for a WebhookEvent Hatif row."""
    kind = hatif_webhook_kind(event.event_name)
    payload = _parse_payload(event.payload_json or "{}")
    sumo = payload.get("_sumo") or {}

    summary = ""
    message_log_id = sumo.get("message_log_id")
    if kind == "whatsapp_status":
        status = delivery_status_from_event(event.event_name)
        summary = delivery_status_label(status)
        if sumo.get("template_name"):
            summary = f"{summary} — {sumo['template_name']}"
    elif kind == "call":
        status_code = _ci(payload, "status")
        try:
            status_code = int(status_code) if status_code is not None else None
        except (TypeError, ValueError):
            status_code = None
        summary = call_status_label(status_code)
        contact = _ci(payload, "contactNumber", "contact_number")
        if contact:
            summary = f"{summary} — {contact}"

    return {
        "event": event,
        "kind": kind,
        "kind_label": hatif_kind_label(kind),
        "summary": summary,
        "message_log_id": message_log_id,
        "template_name": sumo.get("template_name"),
    }


def hatif_event_detail_context(event: Any) -> dict[str, Any]:
    """Build detail-page context for a Hatif webhook event."""
    kind = hatif_webhook_kind(event.event_name)
    payload = _parse_payload(event.payload_json or "{}")
    sumo = payload.get("_sumo") or {}

    fields: list[dict[str, str]] = [
        {"label": "نوع الويبهوك", "value": hatif_kind_label(kind)},
        {"label": "اسم الحدث", "value": event.event_name or "—"},
        {"label": "المعرّف الخارجي", "value": event.external_event_id or "—"},
        {"label": "الجوال", "value": event.phone or "—"},
    ]

    message_log_id = sumo.get("message_log_id")
    if message_log_id:
        fields.append({"label": "سجل الرسالة المرتبط", "value": message_log_id})

    if kind == "whatsapp_status":
        status = delivery_status_from_event(event.event_name)
        fields.extend(
            [
                {"label": "حالة التسليم", "value": delivery_status_label(status)},
                {"label": "الاتجاه", "value": str(_ci(payload, "direction") or "—")},
                {"label": "نوع الرسالة", "value": str(_ci(payload, "messageType", "message_type") or "—")},
                {"label": "معرّف الرسالة", "value": str(_ci(payload, "messageId", "message_id") or "—")},
                {"label": "معرّف المحادثة", "value": str(_ci(payload, "conversationId", "conversation_id") or "—")},
                {"label": "معرّف جهة الاتصال", "value": str(_ci(payload, "contactId", "contact_id") or "—")},
                {"label": "معرّف القناة", "value": str(_ci(payload, "channelId", "channel_id") or "—")},
            ]
        )
        if sumo.get("template_name"):
            fields.append({"label": "قالب واتساب", "value": sumo["template_name"]})
        error_reason = _ci(payload, "errorReason", "error_reason")
        if error_reason:
            fields.append({"label": "سبب الخطأ", "value": str(error_reason)})
    elif kind == "call":
        status_code = _ci(payload, "status")
        direction_code = _ci(payload, "type")
        try:
            status_code = int(status_code) if status_code is not None else None
        except (TypeError, ValueError):
            status_code = None
        try:
            direction_code = int(direction_code) if direction_code is not None else None
        except (TypeError, ValueError):
            direction_code = None
        fields.extend(
            [
                {"label": "حالة المكالمة", "value": call_status_label(status_code)},
                {"label": "الاتجاه", "value": call_direction_label(direction_code)},
                {"label": "المتصل", "value": str(_ci(payload, "callerNumber", "caller_number") or "—")},
                {"label": "المستقبل", "value": str(_ci(payload, "calleeNumber", "callee_number") or "—")},
                {"label": "رقم جهة الاتصال", "value": str(_ci(payload, "contactNumber", "contact_number") or "—")},
                {"label": "مدة المكالمة", "value": str(_ci(payload, "callLength", "call_length") or "—")},
                {"label": "رابط التسجيل", "value": str(_ci(payload, "recordingUrl", "recording_url") or "—")},
            ]
        )
        summary = _ci(payload, "summary")
        if summary:
            fields.append({"label": "ملخص المكالمة", "value": str(summary)})

    return {
        "kind": kind,
        "kind_label": hatif_kind_label(kind),
        "fields": fields,
        "message_log_id": message_log_id,
    }


def apply_hatif_event_filters(stmt, *, kind: str | None, event_name: str | None, phone: str | None, q: str | None):
    """Narrow a WebhookEvent select to Hatif inbound rows."""
    from sqlalchemy import or_

    from app.models import WebhookEvent

    stmt = stmt.where(WebhookEvent.event_name.like(f"{HATIF_WEBHOOK_EVENT_PREFIX}%"))
    if kind and kind != "all":
        if kind == "whatsapp_status":
            stmt = stmt.where(WebhookEvent.event_name.like(f"{HATIF_STATUS_EVENT_PREFIX}%"))
        elif kind == "call":
            stmt = stmt.where(WebhookEvent.event_name.like(f"{HATIF_CALL_EVENT_PREFIX}%"))
    if event_name:
        stmt = stmt.where(WebhookEvent.event_name == event_name.strip())
    if phone:
        stmt = stmt.where(WebhookEvent.phone.contains(phone.strip()))
    if q:
        stmt = stmt.where(
            or_(WebhookEvent.payload_json.contains(q), WebhookEvent.external_event_id.contains(q))
        )
    return stmt


def get_hatif_event_stats(db) -> dict[str, int]:
    from sqlalchemy import func, select

    from app.admin.datetime_ui import riyadh_today_start_utc_naive
    from app.models import WebhookEvent

    today = riyadh_today_start_utc_naive()
    whatsapp_today = (
        select(func.count())
        .select_from(WebhookEvent)
        .where(
            WebhookEvent.created_at >= today,
            WebhookEvent.event_name.like(f"{HATIF_STATUS_EVENT_PREFIX}%"),
        )
    )
    call_today = (
        select(func.count())
        .select_from(WebhookEvent)
        .where(
            WebhookEvent.created_at >= today,
            WebhookEvent.event_name.like(f"{HATIF_CALL_EVENT_PREFIX}%"),
        )
    )
    total = (
        select(func.count())
        .select_from(WebhookEvent)
        .where(WebhookEvent.event_name.like(f"{HATIF_WEBHOOK_EVENT_PREFIX}%"))
    )
    return {
        "total": db.scalar(total) or 0,
        "whatsapp_status_today": db.scalar(whatsapp_today) or 0,
        "call_today": db.scalar(call_today) or 0,
    }


HATIF_EVENT_NAME_HINTS: list[str] = [
    "HatifStatus:Sent",
    "HatifStatus:Delivered",
    "HatifStatus:Read",
    "HatifStatus:Pending",
    "HatifStatus:Failed",
    "HatifCall:Completed",
    "HatifCall:Missed",
    "HatifCall:Failed",
    "HatifCall:NoAnswer",
]
