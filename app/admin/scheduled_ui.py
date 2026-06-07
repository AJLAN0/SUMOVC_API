"""Reminder queue view models for the scheduled messages dashboard."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.admin.datetime_ui import format_riyadh_date, format_riyadh_time, to_riyadh
from app.models import ScheduledMessage


def run_at_iso_utc(run_at: datetime | None) -> str:
    if not run_at:
        return ""
    return as_utc_naive(run_at).strftime("%Y-%m-%dT%H:%M:%SZ")


def as_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _customer_hint(params_json: str) -> str | None:
    try:
        params = json.loads(params_json or "[]")
        if isinstance(params, list) and params:
            first = str(params[0]).strip()
            return first if first and first != "-" else None
    except Exception:
        pass
    return None


def build_reminder_row(job: ScheduledMessage, *, rank: int, now: datetime) -> dict[str, Any]:
    run_at = job.run_at
    is_overdue = bool(run_at and job.status == "pending" and run_at < now)
    local = to_riyadh(run_at)
    return {
        "job": job,
        "rank": rank,
        "is_next": rank == 1 and job.status == "pending",
        "is_overdue": is_overdue,
        "run_at_iso": run_at_iso_utc(run_at),
        "run_at_date": format_riyadh_date(run_at),
        "run_at_time": format_riyadh_time(run_at),
        "run_at_time_sec": format_riyadh_time(run_at, with_seconds=True),
        "reservation_number": job.reservation_number or "—",
        "customer_hint": _customer_hint(job.params_json),
        "to_phone": job.to_phone,
        "template_name": job.template_name,
    }


def get_scheduled_page_data(
    db: Session,
    *,
    status: str | None,
    page: int,
    page_size: int = 25,
) -> dict[str, Any]:
    now = datetime.utcnow()

    pending_items = db.execute(
        select(ScheduledMessage)
        .where(ScheduledMessage.status == "pending")
        .order_by(ScheduledMessage.run_at.asc())
    ).scalars().all()

    pending_rows = [
        build_reminder_row(job, rank=idx + 1, now=now)
        for idx, job in enumerate(pending_items)
    ]
    overdue_count = sum(1 for row in pending_rows if row["is_overdue"])
    next_row = pending_rows[0] if pending_rows else None

    history_stmt = select(ScheduledMessage).where(ScheduledMessage.status != "pending")
    if status and status != "pending":
        history_stmt = select(ScheduledMessage).where(ScheduledMessage.status == status)
    history_stmt = history_stmt.order_by(ScheduledMessage.run_at.desc())

    history_total = db.scalar(select(func.count()).select_from(history_stmt.subquery())) or 0
    history_items = db.execute(
        history_stmt.offset((page - 1) * page_size).limit(page_size)
    ).scalars().all()

    show_queue = not status or status == "pending"
    show_history = not status or status != "pending"

    return {
        "now": now,
        "pending_rows": pending_rows,
        "pending_count": len(pending_rows),
        "overdue_count": overdue_count,
        "next_row": next_row,
        "history_items": history_items,
        "history_total": history_total,
        "page": max(1, page),
        "page_size": page_size,
        "has_more_history": page * page_size < history_total,
        "show_queue": show_queue,
        "show_history": show_history,
        "status": status or "",
    }
