import json
import time
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.config import settings
from app.database import engine
from app.models import (
    EventTemplateMapping,
    MessageLog,
    ScheduledMessage,
    SentNotification,
    WebhookEvent,
)
from app.admin.errors import explain_error, humanize_error
from app.services.rekaz import EVENT_TEMPLATE_MAP

_MAPPING_CACHE: dict[str, str | None] | None = None
_MAPPING_CACHE_AT: float = 0.0
_MAPPING_CACHE_TTL = 30.0

DEFAULT_MAPPING_SEEDS: list[dict[str, Any]] = [
    {
        "event_name": "ReservationConfirmedEvent",
        "template_name": "reservation_confirmedddddddd",
        "enabled": True,
        "description": "Customer confirmation WhatsApp",
        "staff_role": "admin",
        "staff_template_name": "admin_reservation_confirmedddd",
    },
    {
        "event_name": "ReservationCancelledEvent",
        "template_name": "reservation_cancelled",
        "enabled": True,
        "description": "Cancellation WhatsApp",
        "staff_role": None,
        "staff_template_name": None,
    },
    {
        "event_name": "ReservationCreatedEvent",
        "template_name": "reservation_confirmedddddddd",
        "enabled": False,
        "description": "Enable if Rekaz only sends Created (not Confirmed)",
        "staff_role": "admin",
        "staff_template_name": "admin_reservation_confirmedddd",
    },
    {
        "event_name": "GiftCreatedEvent",
        "template_name": "gifft_send",
        "enabled": True,
        "description": "Gift WhatsApp to RecipientCustomer",
        "staff_role": "portrait_technician",
        "staff_template_name": None,
    },
    {
        "event_name": "MerchandiseOrderCreatedEvent",
        "template_name": "",
        "enabled": False,
        "description": "Merchandise order — configure customer template",
        "staff_role": "product_technician",
        "staff_template_name": None,
    },
]


def seed_event_mappings(db: Session) -> None:
    for seed in DEFAULT_MAPPING_SEEDS:
        existing = db.execute(
            select(EventTemplateMapping).where(EventTemplateMapping.event_name == seed["event_name"])
        ).scalar_one_or_none()
        if existing:
            # One-time upgrade: gift mapping may exist disabled with empty template
            if (
                seed["event_name"] == "GiftCreatedEvent"
                and (not existing.template_name or not existing.template_name.strip())
                and seed.get("template_name")
            ):
                existing.template_name = seed["template_name"]
                existing.enabled = seed.get("enabled", existing.enabled)
                existing.staff_role = seed.get("staff_role") or existing.staff_role
                existing.description = seed.get("description") or existing.description
                existing.updated_at = datetime.utcnow()
            continue
        db.add(
            EventTemplateMapping(
                event_name=seed["event_name"],
                template_name=seed["template_name"],
                enabled=seed["enabled"],
                description=seed.get("description"),
                staff_role=seed.get("staff_role"),
                staff_template_name=seed.get("staff_template_name"),
            )
        )
    db.commit()


def get_staff_notification_for_event(db: Session, event_name: str | None) -> tuple[str | None, str | None]:
    """
    Return (staff_role, staff_template_name) for an event.
    Missing staff_role defaults to admin. No staff_template means no staff send.
    """
    if not event_name:
        return None, None

    row = db.execute(
        select(EventTemplateMapping).where(EventTemplateMapping.event_name == event_name)
    ).scalar_one_or_none()
    if row and row.staff_template_name:
        role = row.staff_role or "admin"
        return role, row.staff_template_name

    if event_name in ("ReservationConfirmedEvent", "ReservationCreatedEvent"):
        return "admin", "admin_reservation_confirmedddd"
    return None, None


def invalidate_mapping_cache() -> None:
    global _MAPPING_CACHE, _MAPPING_CACHE_AT
    _MAPPING_CACHE = None
    _MAPPING_CACHE_AT = 0.0


def load_mapping_cache(db: Session) -> dict[str, str | None]:
    global _MAPPING_CACHE, _MAPPING_CACHE_AT
    now = time.time()
    if _MAPPING_CACHE is not None and (now - _MAPPING_CACHE_AT) < _MAPPING_CACHE_TTL:
        return _MAPPING_CACHE

    rows = db.execute(
        select(EventTemplateMapping).where(EventTemplateMapping.enabled.is_(True))
    ).scalars().all()
    cache: dict[str, str | None] = {r.event_name: r.template_name for r in rows}
    if not cache:
        cache = dict(EVENT_TEMPLATE_MAP)

    _MAPPING_CACHE = cache
    _MAPPING_CACHE_AT = now
    return cache


def resolve_template_for_event(db: Session, event_name: str | None) -> str | None:
    if not event_name:
        return None
    cache = load_mapping_cache(db)
    return cache.get(event_name)


def _today_start() -> datetime:
    return datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


def _reservation_from_payload(payload_json: str) -> str | None:
    try:
        data = json.loads(payload_json)
        inner = data.get("Data") or data.get("data") or {}
        return inner.get("number") or inner.get("Number")
    except Exception:
        return None


def get_dashboard_stats(db: Session) -> dict[str, Any]:
    today = _today_start()
    last_24h = datetime.utcnow() - timedelta(hours=24)

    webhooks_today = db.scalar(
        select(func.count()).select_from(WebhookEvent).where(WebhookEvent.created_at >= today)
    ) or 0

    messages_sent_today = db.scalar(
        select(func.count())
        .select_from(MessageLog)
        .where(MessageLog.created_at >= today, MessageLog.status == "success")
    ) or 0

    messages_failed_today = db.scalar(
        select(func.count())
        .select_from(MessageLog)
        .where(MessageLog.created_at >= today, MessageLog.status == "failed")
    ) or 0

    pending_reminders = db.scalar(
        select(func.count())
        .select_from(ScheduledMessage)
        .where(ScheduledMessage.status == "pending")
    ) or 0

    failed_reminders = db.scalar(
        select(func.count())
        .select_from(ScheduledMessage)
        .where(ScheduledMessage.status == "failed")
    ) or 0

    recent_events = db.execute(
        select(WebhookEvent).order_by(WebhookEvent.created_at.desc()).limit(20)
    ).scalars().all()

    recent_rows = []
    for ev in recent_events:
        recent_rows.append(
            {
                "id": ev.id,
                "external_event_id": ev.external_event_id,
                "event_name": ev.event_name,
                "phone": ev.phone,
                "reservation_number": _reservation_from_payload(ev.payload_json),
                "created_at": ev.created_at.isoformat() if ev.created_at else None,
            }
        )

    events_24h = db.execute(
        select(WebhookEvent.created_at).where(WebhookEvent.created_at >= last_24h)
    ).scalars().all()
    hour_totals: dict[str, int] = {}
    for created_at in events_24h:
        if not created_at:
            continue
        hour_key = created_at.strftime("%Y-%m-%d %H:00")
        hour_totals[hour_key] = hour_totals.get(hour_key, 0) + 1
    chart_labels = sorted(hour_totals.keys())[-24:]
    chart_counts = [hour_totals[k] for k in chart_labels]

    last_sched_update = db.scalar(select(func.max(ScheduledMessage.updated_at)))

    now = datetime.utcnow()
    overdue_reminders = db.scalar(
        select(func.count())
        .select_from(ScheduledMessage)
        .where(ScheduledMessage.status == "pending", ScheduledMessage.run_at < now)
    ) or 0

    recent_failed_messages = db.execute(
        select(MessageLog)
        .where(MessageLog.status == "failed")
        .order_by(MessageLog.created_at.desc())
        .limit(5)
    ).scalars().all()

    recent_failed_jobs = db.execute(
        select(ScheduledMessage)
        .where(ScheduledMessage.status == "failed")
        .order_by(ScheduledMessage.updated_at.desc())
        .limit(5)
    ).scalars().all()

    recent_failures: list[dict[str, Any]] = []
    for m in recent_failed_messages:
        raw = m.error_reason or m.provider_response or ""
        ex = explain_error(raw[:500] if raw else None)
        recent_failures.append(
            {
                "kind": "message",
                "id": m.id,
                "at": m.created_at.isoformat() if m.created_at else None,
                "phone": m.phone,
                "template_name": m.template_name,
                "title": ex["title"],
                "message": ex["message"],
                "hint": ex["hint"],
                "summary": humanize_error(raw[:500] if raw else None),
                "url": f"/dashboard/messages/{m.id}",
            }
        )
    for j in recent_failed_jobs:
        ex = explain_error(j.last_error)
        recent_failures.append(
            {
                "kind": "reminder",
                "id": j.id,
                "at": j.updated_at.isoformat() if j.updated_at else None,
                "phone": j.to_phone,
                "template_name": j.template_name,
                "title": ex["title"],
                "message": ex["message"],
                "hint": ex["hint"],
                "summary": humanize_error(j.last_error),
                "url": "/dashboard/scheduled?status=failed",
            }
        )
    recent_failures.sort(key=lambda x: x.get("at") or "", reverse=True)
    recent_failures = recent_failures[:8]

    alerts: list[dict[str, Any]] = []
    if messages_failed_today > 0:
        alerts.append(
            {
                "level": "danger",
                "title": f"{messages_failed_today} رسالة فاشلة اليوم",
                "message": "راجع السبب وصحّح القالب أو بيانات الحجز.",
                "action_url": "/dashboard/messages?status=failed",
                "action_label": "عرض الرسائل الفاشلة",
            }
        )
    if failed_reminders > 0:
        alerts.append(
            {
                "level": "warning",
                "title": f"{failed_reminders} تذكير فاشل",
                "message": "يمكن إعادة المحاولة بعد التأكد من القالب واتصال هاتف.",
                "action_url": "/dashboard/scheduled?status=failed",
                "action_label": "عرض التذكيرات الفاشلة",
            }
        )
    if overdue_reminders > 0:
        alerts.append(
            {
                "level": "warning",
                "title": f"{overdue_reminders} تذكير متأخر",
                "message": "وقت الإرسال مرّ دون إرسال — تحقق أن الخادم يعمل.",
                "action_url": "/dashboard/scheduled?status=pending",
                "action_label": "التذكيرات المعلّقة",
            }
        )
    if pending_reminders > 10:
        alerts.append(
            {
                "level": "info",
                "title": f"{pending_reminders} تذكير في الانتظار",
                "message": "الطابور كبير — طبيعي قبل مواعيد كثيرة.",
                "action_url": "/dashboard/scheduled",
                "action_label": "عرض الجدولة",
            }
        )

    db_health = probe_database()

    return {
        "webhooks_today": webhooks_today,
        "messages_sent_today": messages_sent_today,
        "messages_failed_today": messages_failed_today,
        "pending_reminders": pending_reminders,
        "failed_reminders": failed_reminders,
        "overdue_reminders": overdue_reminders,
        "recent_events": recent_rows,
        "chart_labels": chart_labels,
        "chart_counts": chart_counts,
        "last_scheduled_update": last_sched_update.isoformat() if last_sched_update else None,
        "alerts": alerts,
        "recent_failures": recent_failures,
        "db_health": db_health,
    }


def probe_database() -> dict[str, Any]:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "ok": True,
            "message": "اتصال قاعدة البيانات يعمل",
            "message_ar": "اتصال قاعدة البيانات يعمل",
            "title": "قاعدة البيانات",
        }
    except Exception as exc:
        ex = explain_error(str(exc))
        return {
            "ok": False,
            "message": ex["message"],
            "message_ar": ex["message"],
            "hint": ex["hint"],
            "title": "قاعدة البيانات",
            "raw": str(exc),
        }


async def probe_hatif_token() -> dict[str, Any]:
    try:
        from app.services.hatif import get_access_token

        token = await get_access_token()
        if token:
            return {
                "ok": True,
                "message": "تم الحصول على رمز هاتف بنجاح — الإرسال متاح",
                "message_ar": "تم الحصول على رمز هاتف بنجاح — الإرسال متاح",
                "title": "هاتف (Voxa)",
            }
        ex = explain_error("token empty")
        return {
            "ok": False,
            "message": ex["message"],
            "message_ar": ex["message"],
            "hint": ex["hint"],
            "title": "هاتف (Voxa)",
        }
    except Exception as exc:
        ex = explain_error(str(exc))
        return {
            "ok": False,
            "message": ex["message"],
            "message_ar": ex["message"],
            "hint": ex["hint"],
            "title": "هاتف (Voxa)",
            "raw": str(exc),
        }
