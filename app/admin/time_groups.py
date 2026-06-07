"""Group admin table rows by calendar day for timeline-style lists."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable, TypeVar

T = TypeVar("T")


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


def date_bucket_parts(dt: datetime | None) -> tuple[str, str | None]:
    """Return (section label, optional ISO date shown beside label for today/yesterday)."""
    if not dt:
        return ("بدون تاريخ", None)
    day = dt.date()
    today = datetime.utcnow().date()
    iso = day.strftime("%Y-%m-%d")
    if day == today:
        return ("اليوم", iso)
    if day == today - timedelta(days=1):
        return ("أمس", iso)
    return (iso, None)


def date_bucket_label(dt: datetime | None) -> str:
    label, _ = date_bucket_parts(dt)
    return label


def group_rows_by_time(
    rows: list[T],
    *,
    get_dt: Callable[[T], datetime | None],
) -> list[tuple[str, str | None, list[T]]]:
    """Group consecutive rows sharing the same calendar day (rows must be newest-first)."""
    groups: list[tuple[str, str | None, list[T]]] = []
    current_key: str | None = None
    for row in rows:
        label, date_hint = date_bucket_parts(get_dt(row))
        key = date_hint or label
        if not groups or key != current_key:
            groups.append((label, date_hint, []))
            current_key = key
        groups[-1][2].append(row)
    return groups
