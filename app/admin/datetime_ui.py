"""Display datetimes in Asia/Riyadh (stored values are UTC)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

RIYADH_TZ = ZoneInfo("Asia/Riyadh")
DISPLAY_TZ_LABEL_AR = "توقيت الرياض"


def coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        raw = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(raw.replace("+00:00", ""))
        except ValueError:
            return None
    return None


def as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_riyadh(value: Any) -> datetime | None:
    dt = coerce_datetime(value) if not isinstance(value, datetime) else value
    if not dt:
        return None
    return as_utc(dt).astimezone(RIYADH_TZ)


def riyadh_now() -> datetime:
    return datetime.now(RIYADH_TZ)


def riyadh_today_start_utc_naive() -> datetime:
    """UTC-naive instant for midnight today in Riyadh (matches DB storage)."""
    local_start = riyadh_now().replace(hour=0, minute=0, second=0, microsecond=0)
    return local_start.astimezone(timezone.utc).replace(tzinfo=None)


def format_riyadh_date(value: Any) -> str:
    local = to_riyadh(value)
    return local.strftime("%Y-%m-%d") if local else ""


def format_riyadh_time(value: Any, *, with_seconds: bool = False) -> str:
    local = to_riyadh(value)
    if not local:
        return ""
    return local.strftime("%H:%M:%S" if with_seconds else "%H:%M")


def date_bucket_parts(value: Any) -> tuple[str, str | None]:
    """Return (section label, optional ISO date beside label for today/yesterday)."""
    local = to_riyadh(value)
    if not local:
        return ("بدون تاريخ", None)
    day = local.date()
    today = riyadh_now().date()
    iso = day.strftime("%Y-%m-%d")
    if day == today:
        return ("اليوم", iso)
    if day == today - timedelta(days=1):
        return ("أمس", iso)
    return (iso, None)
