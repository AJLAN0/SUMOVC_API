"""Group admin table rows by calendar day for timeline-style lists."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, TypeVar

from app.admin.datetime_ui import coerce_datetime, date_bucket_parts

T = TypeVar("T")

__all__ = ["coerce_datetime", "date_bucket_label", "group_rows_by_time"]


def date_bucket_label(dt: datetime | None) -> str:
    label, _ = date_bucket_parts(dt)
    return label


def group_rows_by_time(
    rows: list[T],
    *,
    get_dt: Callable[[T], datetime | None],
) -> list[tuple[str, str | None, list[T]]]:
    """Group consecutive rows sharing the same Riyadh calendar day (newest-first)."""
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
