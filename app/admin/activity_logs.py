"""Unified activity timeline for the admin dashboard."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.admin.errors import explain_error, humanize_error
from app.admin.rekaz_ui import kind_label, payload_kind_for_event
from app.admin.datetime_ui import (
    format_riyadh_date,
    format_riyadh_time,
    riyadh_today_start_utc_naive,
)
from app.models import MessageLog, ScheduledMessage, SentNotification, WebhookEvent

LOG_TYPE_LABELS_AR: dict[str, str] = {
    "webhook": "حدث وارد",
    "message": "رسالة واتساب",
    "scheduled": "تذكير مجدول",
    "lock": "منع تكرار",
}

LOG_TYPE_DESCRIPTIONS_AR: dict[str, str] = {
    "webhook": "طلب وارد من ركاز (حجز، هدية، منتجات، اشتراك)",
    "message": "محاولة إرسال واتساب عبر هاتف",
    "scheduled": "تذكير مجدول للإرسال لاحقاً",
    "lock": "تسجيل لمنع إرسال نفس الرسالة مرتين",
}

STATUS_LABELS_AR: dict[str, str] = {
    "success": "نجح",
    "failed": "فشل",
    "pending": "قيد الانتظار",
    "sent": "تم الإرسال",
    "canceled": "ملغى",
    "duplicate": "مكرر (تم التخطي)",
    "skipped": "تم التخطي (لا تغيير بالموعد)",
    "received": "تم الاستلام",
    "locked": "مقفل",
}

_KIND_PREFIXES: dict[str, tuple[str, ...]] = {
    "reservation": ("Reservation%",),
    "gift": ("Gift%",),
    "merchandise": ("Merchandise%",),
    "subscription": ("Subscription%",),
}


def _today_start() -> datetime:
    return riyadh_today_start_utc_naive()


def _webhook_kind_filter(stmt, kind: str | None):
    if not kind or kind == "all":
        return stmt
    prefixes = _KIND_PREFIXES.get(kind)
    if not prefixes:
        return stmt
    clauses = [WebhookEvent.event_name.like(p) for p in prefixes]
    return stmt.where(or_(*clauses))


def _entry_detail_url(log_type: str, row_id: str) -> str:
    routes = {
        "webhook": f"/dashboard/events/{row_id}",
        "message": f"/dashboard/messages/{row_id}",
        "scheduled": "/dashboard/scheduled",
        "lock": "/dashboard/locks",
    }
    return routes.get(log_type, "/dashboard/logs")


def _display_lines(
    *,
    action: str,
    primary_label: str,
    primary_value: str,
    secondary_label: str | None = None,
    secondary_value: str | None = None,
    note: str = "",
    error_title: str = "",
    error_message: str = "",
) -> dict[str, str | None]:
    return {
        "action_text": action,
        "primary_label": primary_label,
        "primary_value": primary_value or "—",
        "secondary_label": secondary_label,
        "secondary_value": secondary_value or None,
        "note": note,
        "error_title": error_title,
        "error_message": error_message,
    }


def _webhook_entries(db: Session, phone: str | None, q: str | None, kind: str | None, limit: int) -> list[dict]:
    stmt = select(WebhookEvent).order_by(WebhookEvent.created_at.desc()).limit(limit)
    if phone:
        stmt = stmt.where(WebhookEvent.phone.contains(phone))
    if q:
        stmt = stmt.where(
            or_(WebhookEvent.payload_json.contains(q), WebhookEvent.external_event_id.contains(q))
        )
    stmt = _webhook_kind_filter(stmt, kind)
    rows = db.execute(stmt).scalars().all()
    out: list[dict] = []
    for row in rows:
        pk = payload_kind_for_event(row.event_name)
        kl = kind_label(pk)
        display = _display_lines(
            action=f"استلام إشعار من ركاز — {kl}",
            primary_label="حدث ركاز",
            primary_value=row.event_name or "—",
            secondary_label="المعرّف",
            secondary_value=row.external_event_id,
        )
        out.append(
            {
                "log_type": "webhook",
                "log_type_label": LOG_TYPE_LABELS_AR["webhook"],
                "id": row.id,
                "at": row.created_at,
                "title": row.event_name or "—",
                "subtitle": row.external_event_id or "",
                "phone": row.phone,
                "status": "received",
                "status_label": STATUS_LABELS_AR["received"],
                "kind_label": kl,
                "payload_kind": pk.value,
                "detail_url": _entry_detail_url("webhook", row.id),
                "summary": display["action_text"],
                **display,
            }
        )
    return out


def _message_entries(db: Session, phone: str | None, status: str | None, q: str | None, limit: int) -> list[dict]:
    stmt = select(MessageLog).order_by(MessageLog.created_at.desc()).limit(limit)
    if phone:
        stmt = stmt.where(MessageLog.phone.contains(phone))
    if status:
        stmt = stmt.where(MessageLog.status == status)
    if q:
        stmt = stmt.where(
            or_(
                MessageLog.template_name.contains(q),
                MessageLog.provider_response.contains(q),
                MessageLog.error_reason.contains(q),
            )
        )
    rows = db.execute(stmt).scalars().all()
    out: list[dict] = []
    for row in rows:
        err = explain_error((row.error_reason or row.provider_response or "")[:300] if row.status == "failed" else None)
        action = "فشل إرسال واتساب" if row.status == "failed" else "إرسال واتساب للعميل"
        if row.status == "duplicate":
            action = "تخطي رسالة مكررة"
        display = _display_lines(
            action=action,
            primary_label="قالب واتساب",
            primary_value=row.template_name or "—",
            secondary_label="حالة التسليم" if row.last_status else None,
            secondary_value=row.last_status,
            note=humanize_error(row.error_reason or row.provider_response) if row.status == "failed" else "",
            error_title=err["title"] if row.status == "failed" else "",
            error_message=err["message"] if row.status == "failed" else "",
        )
        out.append(
            {
                "log_type": "message",
                "log_type_label": LOG_TYPE_LABELS_AR["message"],
                "id": row.id,
                "at": row.created_at,
                "title": row.template_name or "—",
                "subtitle": row.last_status or "",
                "phone": row.phone,
                "status": row.status,
                "status_label": STATUS_LABELS_AR.get(row.status, row.status),
                "kind_label": None,
                "detail_url": _entry_detail_url("message", row.id),
                "summary": display["note"] or action,
                **display,
            }
        )
    return out


def _scheduled_entries(db: Session, phone: str | None, status: str | None, q: str | None, limit: int) -> list[dict]:
    stmt = select(ScheduledMessage).order_by(ScheduledMessage.updated_at.desc()).limit(limit)
    if phone:
        stmt = stmt.where(ScheduledMessage.to_phone.contains(phone))
    if status:
        stmt = stmt.where(ScheduledMessage.status == status)
    if q:
        stmt = stmt.where(
            or_(
                ScheduledMessage.template_name.contains(q),
                ScheduledMessage.reservation_number.contains(q),
            )
        )
    rows = db.execute(stmt).scalars().all()
    out: list[dict] = []
    for row in rows:
        when = (
            f"{format_riyadh_date(row.run_at)} {format_riyadh_time(row.run_at)}"
            if row.run_at
            else "—"
        )
        status_note = {
            "pending": f"سيُرسل في {when}",
            "sent": "تم الإرسال في الموعد",
            "failed": humanize_error(row.last_error),
            "canceled": "تم إلغاء التذكير",
        }.get(row.status, "")
        display = _display_lines(
            action="تذكير موعد للعميل",
            primary_label="قالب التذكير",
            primary_value=row.template_name or "—",
            secondary_label="رقم الحجز",
            secondary_value=row.reservation_number,
            note=status_note,
            error_message=humanize_error(row.last_error) if row.status == "failed" else "",
        )
        out.append(
            {
                "log_type": "scheduled",
                "log_type_label": LOG_TYPE_LABELS_AR["scheduled"],
                "id": row.id,
                "at": row.updated_at or row.created_at,
                "title": row.template_name,
                "subtitle": f"موعد الإرسال: {when}",
                "phone": row.to_phone,
                "status": row.status,
                "status_label": STATUS_LABELS_AR.get(row.status, row.status),
                "kind_label": None,
                "detail_url": _entry_detail_url("scheduled", row.id),
                "summary": row.reservation_number or when,
                "run_at_label": when,
                **display,
            }
        )
    return out


def _lock_entries(db: Session, phone: str | None, q: str | None, limit: int) -> list[dict]:
    stmt = select(SentNotification).order_by(SentNotification.created_at.desc()).limit(limit)
    if phone:
        stmt = stmt.where(SentNotification.phone.contains(phone))
    if q:
        stmt = stmt.where(
            or_(
                SentNotification.reservation_number.contains(q),
                SentNotification.notification_type.contains(q),
            )
        )
    rows = db.execute(stmt).scalars().all()
    out: list[dict] = []
    for row in rows:
        display = _display_lines(
            action="منع إرسال مكرر",
            primary_label="نوع الإشعار",
            primary_value=row.notification_type or "—",
            secondary_label="رقم الحجز",
            secondary_value=row.reservation_number,
            note="تم تسجيل هذا الإرسال مسبقاً ولن يُكرر",
        )
        out.append(
            {
                "log_type": "lock",
                "log_type_label": LOG_TYPE_LABELS_AR["lock"],
                "id": row.id,
                "at": row.created_at,
                "title": row.notification_type,
                "subtitle": row.reservation_number,
                "phone": row.phone,
                "status": "locked",
                "status_label": STATUS_LABELS_AR["locked"],
                "kind_label": None,
                "detail_url": _entry_detail_url("lock", row.id),
                "summary": display["note"],
                **display,
            }
        )
    return out


def get_activity_stats(db: Session) -> dict[str, int]:
    today = _today_start()
    return {
        "webhooks_today": db.scalar(
            select(func.count()).select_from(WebhookEvent).where(WebhookEvent.created_at >= today)
        )
        or 0,
        "messages_sent_today": db.scalar(
            select(func.count())
            .select_from(MessageLog)
            .where(MessageLog.created_at >= today, MessageLog.status == "success")
        )
        or 0,
        "messages_failed_today": db.scalar(
            select(func.count())
            .select_from(MessageLog)
            .where(MessageLog.created_at >= today, MessageLog.status == "failed")
        )
        or 0,
        "pending_reminders": db.scalar(
            select(func.count()).select_from(ScheduledMessage).where(ScheduledMessage.status == "pending")
        )
        or 0,
    }


def get_activity_logs(
    db: Session,
    *,
    page: int = 1,
    page_size: int = 30,
    log_type: str = "all",
    status: str | None = None,
    phone: str | None = None,
    q: str | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    """Merge recent rows from all log sources into one timeline."""
    fetch_limit = page_size * 3
    entries: list[dict] = []

    if log_type in ("all", "webhook"):
        entries.extend(_webhook_entries(db, phone, q, kind if log_type in ("all", "webhook") else None, fetch_limit))
    if log_type in ("all", "message"):
        msg_status = status if status and status not in ("received", "locked") else None
        entries.extend(_message_entries(db, phone, msg_status, q, fetch_limit))
    if log_type in ("all", "scheduled"):
        sched_status = status if status and status in ("pending", "sent", "failed", "canceled") else None
        entries.extend(_scheduled_entries(db, phone, sched_status, q, fetch_limit))
    if log_type in ("all", "lock"):
        if not status or status == "locked":
            entries.extend(_lock_entries(db, phone, q, fetch_limit))

    entries.sort(key=lambda x: x["at"] or datetime.min, reverse=True)

    total = len(entries)
    start = (page - 1) * page_size
    page_items = entries[start : start + page_size]

    return {
        "items": page_items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_more": start + page_size < total,
    }
