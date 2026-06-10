"""Client-centric send history (customer + related staff/admin messages)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.admin.rekaz_ui import kind_label, payload_kind_for_event
from app.models import (
    EventTemplateMapping,
    MessageLog,
    RoleRecipient,
    ScheduledMessage,
    SentNotification,
    WebhookEvent,
)
from app.services.rekaz import normalize_phone
from app.services.role_recipients import role_display_meta
from app.services.template_catalog import ENGLISH_TEMPLATE_NAMES

RECIPIENT_LABELS_AR: dict[str, str] = {
    "customer": "العميل",
    "staff": "الموظفون",
    "webhook": "حدث وارد",
    "scheduled": "تذكير",
}

ENTRY_KIND_LABELS_AR: dict[str, str] = {
    "customer_message": "رسالة للعميل",
    "staff_message": "رسالة للموظفين",
    "scheduled": "تذكير مجدول",
    "webhook": "حدث من ركاز",
}


def _staff_phone_set(db: Session) -> set[str]:
    rows = db.execute(
        select(RoleRecipient.phone).where(RoleRecipient.enabled.is_(True))
    ).scalars().all()
    out: set[str] = set()
    for phone in rows:
        norm = normalize_phone(phone)
        if norm:
            out.add(norm)
    return out


def _staff_templates(db: Session) -> set[str]:
    rows = db.execute(
        select(EventTemplateMapping.staff_template_name).where(
            EventTemplateMapping.staff_template_name.isnot(None)
        )
    ).scalars().all()
    return {t for t in rows if t} | set(ENGLISH_TEMPLATE_NAMES)


def _staff_role_for_phone(db: Session, phone: str) -> str:
    norm = normalize_phone(phone) or phone
    rows = db.execute(
        select(RoleRecipient).where(RoleRecipient.enabled.is_(True))
    ).scalars().all()
    for row in rows:
        if normalize_phone(row.phone) == norm:
            meta = role_display_meta(row.role)
            return row.label or meta.get("label_ar") or row.role
    return "موظف"


def _reservation_from_payload(payload_json: str) -> str | None:
    try:
        data = json.loads(payload_json)
        inner = data.get("Data") or data.get("data") or {}
        return (
            inner.get("number")
            or inner.get("Number")
            or inner.get("orderCode")
            or inner.get("OrderCode")
        )
    except Exception:
        return None


def _correlation_refs_from_event(ev: WebhookEvent) -> set[str]:
    refs: set[str] = set()
    if ev.external_event_id:
        refs.add(ev.external_event_id)
    res = _reservation_from_payload(ev.payload_json)
    if res:
        refs.add(str(res))
    return refs


def _phone_matches(client_norm: str, raw: str | None) -> bool:
    if not raw:
        return False
    return normalize_phone(raw) == client_norm


def _merge_client_row(
    index: dict[str, dict[str, Any]],
    raw_phone: str | None,
    at: datetime | None,
    *,
    staff_phones: set[str],
    bump_webhook: int = 0,
    bump_message: int = 0,
    bump_scheduled: int = 0,
) -> None:
    norm = normalize_phone(raw_phone)
    if not norm or norm in staff_phones:
        return
    row = index.get(norm)
    if not row:
        row = {
            "phone": norm,
            "phone_display": raw_phone or norm,
            "last_at": at,
            "webhook_count": 0,
            "message_count": 0,
            "scheduled_count": 0,
        }
        index[norm] = row
    else:
        row["phone_display"] = raw_phone or row["phone_display"]
        if at and (not row["last_at"] or at > row["last_at"]):
            row["last_at"] = at
    row["webhook_count"] += bump_webhook
    row["message_count"] += bump_message
    row["scheduled_count"] += bump_scheduled


def list_clients(
    db: Session,
    *,
    page: int = 1,
    page_size: int = 50,
    q: str | None = None,
) -> dict[str, Any]:
    staff_phones = _staff_phone_set(db)
    index: dict[str, dict[str, Any]] = {}

    for ev in db.execute(
        select(WebhookEvent).where(WebhookEvent.phone.isnot(None)).order_by(WebhookEvent.created_at.desc())
    ).scalars():
        _merge_client_row(
            index, ev.phone, ev.created_at,
            staff_phones=staff_phones, bump_webhook=1,
        )

    for msg in db.execute(
        select(MessageLog).where(MessageLog.phone.isnot(None)).order_by(MessageLog.created_at.desc())
    ).scalars():
        norm = normalize_phone(msg.phone)
        if norm and norm not in staff_phones:
            _merge_client_row(
                index, msg.phone, msg.created_at,
                staff_phones=staff_phones, bump_message=1,
            )

    for job in db.execute(select(ScheduledMessage).order_by(ScheduledMessage.created_at.desc())).scalars():
        _merge_client_row(
            index, job.to_phone, job.updated_at or job.created_at,
            staff_phones=staff_phones, bump_scheduled=1,
        )

    items = sorted(
        index.values(),
        key=lambda r: r["last_at"] or datetime.min,
        reverse=True,
    )

    if q:
        q_digits = "".join(c for c in q if c.isdigit())
        q_lower = q.strip().lower()
        items = [
            r for r in items
            if q_lower in (r["phone_display"] or "").lower()
            or q_lower in r["phone"]
            or (q_digits and q_digits in r["phone"])
        ]

    total = len(items)
    page = max(1, page)
    page_size = min(max(page_size, 10), 200)
    start = (page - 1) * page_size
    page_items = items[start : start + page_size]

    return {
        "items": page_items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_more": start + page_size < total,
    }


def get_client_profile(db: Session, client_phone: str) -> dict[str, Any] | None:
    client_norm = normalize_phone(client_phone)
    if not client_norm:
        return None

    staff_phones = _staff_phone_set(db)
    if client_norm in staff_phones:
        return None

    webhooks = [
        ev for ev in db.execute(
            select(WebhookEvent)
            .where(WebhookEvent.phone.isnot(None))
            .order_by(WebhookEvent.created_at.desc())
        ).scalars()
        if _phone_matches(client_norm, ev.phone)
    ]

    message_count = sum(
        1
        for msg in db.execute(select(MessageLog).where(MessageLog.phone.isnot(None))).scalars()
        if normalize_phone(msg.phone) == client_norm
    )
    scheduled_count = sum(
        1
        for job in db.execute(select(ScheduledMessage)).scalars()
        if _phone_matches(client_norm, job.to_phone)
    )

    if not webhooks and not message_count and not scheduled_count:
        return None

    correlation_refs: set[str] = set()
    for ev in webhooks:
        correlation_refs |= _correlation_refs_from_event(ev)

    display_phone = webhooks[0].phone if webhooks else client_norm

    return {
        "phone": client_norm,
        "phone_display": display_phone,
        "webhook_count": len(webhooks),
        "message_count": message_count,
        "scheduled_count": scheduled_count,
        "correlation_refs": sorted(correlation_refs),
    }


def get_client_history(
    db: Session,
    client_phone: str,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    client_norm = normalize_phone(client_phone)
    if not client_norm:
        return []

    staff_phones = _staff_phone_set(db)
    staff_templates = _staff_templates(db)
    entries: list[dict[str, Any]] = []
    seen_message_ids: set[str] = set()

    webhooks = [
        ev for ev in db.execute(
            select(WebhookEvent)
            .where(WebhookEvent.phone.isnot(None))
            .order_by(WebhookEvent.created_at.desc())
            .limit(500)
        ).scalars()
        if _phone_matches(client_norm, ev.phone)
    ]

    correlation_refs: set[str] = set()
    for ev in webhooks:
        correlation_refs |= _correlation_refs_from_event(ev)
        pk = payload_kind_for_event(ev.event_name)
        entries.append(
            {
                "at": ev.created_at,
                "entry_kind": "webhook",
                "entry_kind_label": ENTRY_KIND_LABELS_AR["webhook"],
                "recipient_kind": "webhook",
                "recipient_label": RECIPIENT_LABELS_AR["webhook"],
                "title": ev.event_name or "—",
                "subtitle": ev.external_event_id or _reservation_from_payload(ev.payload_json) or "",
                "status": "received",
                "template_name": None,
                "to_phone": ev.phone,
                "detail_url": f"/dashboard/events/{ev.id}",
                "payload_kind_label": kind_label(pk),
            }
        )

    # Customer messages
    for msg in db.execute(
        select(MessageLog)
        .where(MessageLog.phone.isnot(None))
        .order_by(MessageLog.created_at.desc())
        .limit(500)
    ).scalars():
        norm = normalize_phone(msg.phone)
        if norm == client_norm:
            seen_message_ids.add(msg.id)
            entries.append(_message_entry(msg, recipient_kind="customer", recipient_label=RECIPIENT_LABELS_AR["customer"]))

    # Scheduled reminders to client
    for job in db.execute(
        select(ScheduledMessage).order_by(ScheduledMessage.updated_at.desc()).limit(200)
    ).scalars():
        if _phone_matches(client_norm, job.to_phone):
            entries.append(
                {
                    "at": job.updated_at or job.created_at,
                    "entry_kind": "scheduled",
                    "entry_kind_label": ENTRY_KIND_LABELS_AR["scheduled"],
                    "recipient_kind": "scheduled",
                    "recipient_label": RECIPIENT_LABELS_AR["scheduled"],
                    "title": job.template_name,
                    "subtitle": job.reservation_number or "",
                    "status": job.status,
                    "template_name": job.template_name,
                    "to_phone": job.to_phone,
                    "detail_url": "/dashboard/scheduled",
                }
            )

    # Staff/admin messages linked via webhook time window
    for ev in webhooks:
        if not ev.created_at:
            continue
        window_end = ev.created_at + timedelta(seconds=120)
        staff_logs = db.execute(
            select(MessageLog).where(
                MessageLog.created_at >= ev.created_at,
                MessageLog.created_at <= window_end,
            )
        ).scalars()
        for msg in staff_logs:
            if msg.id in seen_message_ids:
                continue
            norm = normalize_phone(msg.phone)
            if norm not in staff_phones:
                continue
            if staff_templates and msg.template_name not in staff_templates:
                continue
            seen_message_ids.add(msg.id)
            role_label = _staff_role_for_phone(db, msg.phone or "")
            entries.append(
                _message_entry(
                    msg,
                    recipient_kind="staff",
                    recipient_label=role_label,
                    entry_kind_label=ENTRY_KIND_LABELS_AR["staff_message"],
                )
            )

    # Staff messages linked via SentNotification correlation refs
    if correlation_refs:
        ref_list = list(correlation_refs)
        sn_stmt = select(SentNotification).where(
            or_(
                SentNotification.reservation_number.in_(ref_list),
                *[SentNotification.notification_type.contains(ref) for ref in ref_list[:20]],
            )
        )
        for sn in db.execute(sn_stmt).scalars():
            sn_norm = normalize_phone(sn.phone)
            if sn_norm not in staff_phones:
                continue
            window_start = sn.created_at - timedelta(seconds=5)
            window_end = sn.created_at + timedelta(seconds=30)
            for msg in db.execute(
                select(MessageLog).where(
                    MessageLog.phone == sn.phone,
                    MessageLog.created_at >= window_start,
                    MessageLog.created_at <= window_end,
                )
            ).scalars():
                if msg.id in seen_message_ids:
                    continue
                if staff_templates and msg.template_name not in staff_templates:
                    continue
                seen_message_ids.add(msg.id)
                role_label = _staff_role_for_phone(db, msg.phone or "")
                entries.append(
                    _message_entry(
                        msg,
                        recipient_kind="staff",
                        recipient_label=role_label,
                        entry_kind_label=ENTRY_KIND_LABELS_AR["staff_message"],
                    )
                )

    entries.sort(key=lambda e: e["at"] or datetime.min, reverse=True)
    return entries[:limit]


def _message_entry(
    msg: MessageLog,
    *,
    recipient_kind: str,
    recipient_label: str,
    entry_kind_label: str | None = None,
) -> dict[str, Any]:
    is_staff = recipient_kind == "staff"
    return {
        "at": msg.created_at,
        "entry_kind": "staff_message" if is_staff else "customer_message",
        "entry_kind_label": entry_kind_label or (
            ENTRY_KIND_LABELS_AR["staff_message"] if is_staff else ENTRY_KIND_LABELS_AR["customer_message"]
        ),
        "recipient_kind": recipient_kind,
        "recipient_label": recipient_label,
        "title": msg.template_name or "—",
        "subtitle": (msg.phone or "") if is_staff else (msg.last_status or ""),
        "status": msg.status,
        "template_name": msg.template_name,
        "to_phone": msg.phone,
        "detail_url": f"/dashboard/messages/{msg.id}",
    }
