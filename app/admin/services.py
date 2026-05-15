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
    },
    {
        "event_name": "ReservationCancelledEvent",
        "template_name": "reservation_cancelled",
        "enabled": True,
        "description": "Cancellation WhatsApp",
    },
    {
        "event_name": "ReservationCreatedEvent",
        "template_name": "reservation_confirmedddddddd",
        "enabled": False,
        "description": "Enable if Rekaz only sends Created (not Confirmed)",
    },
]


def seed_event_mappings(db: Session) -> None:
    for seed in DEFAULT_MAPPING_SEEDS:
        existing = db.execute(
            select(EventTemplateMapping).where(EventTemplateMapping.event_name == seed["event_name"])
        ).scalar_one_or_none()
        if existing:
            continue
        db.add(
            EventTemplateMapping(
                event_name=seed["event_name"],
                template_name=seed["template_name"],
                enabled=seed["enabled"],
                description=seed.get("description"),
            )
        )
    db.commit()


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

    return {
        "webhooks_today": webhooks_today,
        "messages_sent_today": messages_sent_today,
        "messages_failed_today": messages_failed_today,
        "pending_reminders": pending_reminders,
        "failed_reminders": failed_reminders,
        "recent_events": recent_rows,
        "chart_labels": chart_labels,
        "chart_counts": chart_counts,
        "last_scheduled_update": last_sched_update.isoformat() if last_sched_update else None,
    }


def probe_database() -> dict[str, Any]:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True, "message": "Database connection OK"}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


async def probe_hatif_token() -> dict[str, Any]:
    try:
        from app.services.hatif import get_access_token

        token = await get_access_token()
        return {
            "ok": bool(token),
            "message": "Hatif access token acquired",
        }
    except Exception as exc:
        return {"ok": False, "message": str(exc)}
