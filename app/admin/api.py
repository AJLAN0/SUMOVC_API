import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.admin.auth import require_admin_api
from app.admin.services import (
    get_dashboard_stats,
    invalidate_mapping_cache,
    probe_database,
    probe_hatif_token,
)
from app.database import get_db
from app.models import (
    EventTemplateMapping,
    MessageLog,
    RoleRecipient,
    ScheduledMessage,
    SentNotification,
    WebhookEvent,
    WhatsAppTemplate,
)
from app.services.role_recipients import (
    NOTIFICATION_ROLES,
    add_recipient,
    invalidate_role_cache,
    is_valid_role,
    list_recipients_by_role,
)
from app.services.runtime_settings import (
    get_runtime_settings_view,
    invalidate_settings_cache,
    set_setting,
    SETTING_ALLOWED_LATE_MINUTES,
    SETTING_REMINDER_BEFORE_MINUTES,
)
from app.services.template_catalog import (
    invalidate_template_cache,
    list_enabled_templates,
    param_keys_from_json,
    param_keys_to_json,
    _parse_param_keys,
)

router = APIRouter(prefix="/admin/api", tags=["admin-api"])


class MappingCreate(BaseModel):
    event_name: str
    template_name: str
    enabled: bool = True
    description: str | None = None
    staff_role: str | None = None
    staff_template_name: str | None = None


class MappingUpdate(BaseModel):
    event_name: str | None = None
    template_name: str | None = None
    enabled: bool | None = None
    description: str | None = None
    staff_role: str | None = None
    staff_template_name: str | None = None


class ScheduledCreate(BaseModel):
    to_phone: str
    template_name: str
    params_json: str = "[]"
    run_at: datetime
    reservation_number: str | None = None
    external_event_id: str | None = None
    status: str = "pending"


class ScheduledUpdate(BaseModel):
    to_phone: str | None = None
    template_name: str | None = None
    params_json: str | None = None
    run_at: datetime | None = None
    reservation_number: str | None = None
    status: str | None = None
    attempts: int | None = None
    last_error: str | None = None


class RuntimeSettingsUpdate(BaseModel):
    reminder_before_minutes: int
    allowed_late_minutes: int


class RecipientCreate(BaseModel):
    role: str
    phone: str
    label: str | None = None
    enabled: bool = True


class RecipientUpdate(BaseModel):
    label: str | None = None
    enabled: bool | None = None


@router.get("/stats")
def api_stats(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    return get_dashboard_stats(db)


@router.get("/health/db")
def api_health_db(_: str = Depends(require_admin_api)):
    return probe_database()


@router.get("/health/hatif")
async def api_health_hatif(_: str = Depends(require_admin_api)):
    return await probe_hatif_token()


@router.get("/template-specs")
def api_template_specs(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    items = list_enabled_templates(db)
    return {"templates": [{"name": t["name"], "title_ar": t["title_ar"]} for t in items if t.get("enabled")]}


class TemplateCreate(BaseModel):
    name: str
    title_ar: str | None = None
    description: str | None = None
    param_keys_text: str  # one per line
    enabled: bool = True


class TemplateUpdate(BaseModel):
    title_ar: str | None = None
    description: str | None = None
    param_keys_text: str | None = None
    enabled: bool | None = None


@router.get("/templates")
def api_list_templates(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    return {"items": list_enabled_templates(db)}


@router.get("/templates/{template_id}")
def api_get_template(
    template_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(WhatsAppTemplate, template_id)
    if not row:
        raise HTTPException(404, "Template not found")
    keys = param_keys_from_json(row.param_keys_json)
    return {
        "id": row.id,
        "name": row.name,
        "title_ar": row.title_ar,
        "description": row.description,
        "param_keys": keys,
        "param_keys_text": "\n".join(keys),
        "enabled": row.enabled,
    }


@router.post("/templates")
def api_create_template(
    body: TemplateCreate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    keys = _parse_param_keys(body.param_keys_text)
    if not keys:
        raise HTTPException(
            400,
            detail={
                "message_ar": "أضف متغيراً واحداً على الأقل (سطر لكل متغير).",
                "hint": "مثال: customer_name ثم branch_name في أسطر منفصلة.",
            },
        )
    row = WhatsAppTemplate(
        name=body.name.strip(),
        title_ar=(body.title_ar or "").strip() or None,
        description=body.description,
        param_keys_json=param_keys_to_json(keys),
        enabled=body.enabled,
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(400, detail=str(exc)) from exc
    invalidate_template_cache()
    db.refresh(row)
    return {"id": row.id}


@router.patch("/templates/{template_id}")
def api_update_template(
    template_id: str,
    body: TemplateUpdate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(WhatsAppTemplate, template_id)
    if not row:
        raise HTTPException(404, "Template not found")
    data = body.model_dump(exclude_unset=True)
    if "param_keys_text" in data:
        keys = _parse_param_keys(data.pop("param_keys_text") or "")
        if not keys:
            raise HTTPException(
            400,
            detail={
                "message_ar": "أضف متغيراً واحداً على الأقل (سطر لكل متغير).",
                "hint": "مثال: customer_name ثم branch_name في أسطر منفصلة.",
            },
        )
        row.param_keys_json = param_keys_to_json(keys)
    for field, value in data.items():
        setattr(row, field, value)
    row.updated_at = datetime.utcnow()
    db.commit()
    invalidate_template_cache()
    return {"ok": True}


@router.delete("/templates/{template_id}")
def api_delete_template(
    template_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(WhatsAppTemplate, template_id)
    if not row:
        raise HTTPException(404, "Template not found")
    db.delete(row)
    db.commit()
    invalidate_template_cache()
    return {"ok": True}


# ── Webhook events ──────────────────────────────────────────────────────

@router.get("/events")
def api_list_events(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    event_name: str | None = None,
    phone: str | None = None,
    q: str | None = None,
):
    stmt = select(WebhookEvent)
    if event_name:
        stmt = stmt.where(WebhookEvent.event_name == event_name)
    if phone:
        stmt = stmt.where(WebhookEvent.phone.contains(phone))
    if q:
        stmt = stmt.where(
            or_(
                WebhookEvent.payload_json.contains(q),
                WebhookEvent.external_event_id.contains(q),
            )
        )
    stmt = stmt.order_by(WebhookEvent.created_at.desc())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = db.execute(stmt.offset((page - 1) * page_size).limit(page_size)).scalars().all()
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": [
            {
                "id": i.id,
                "external_event_id": i.external_event_id,
                "event_name": i.event_name,
                "phone": i.phone,
                "created_at": i.created_at.isoformat() if i.created_at else None,
            }
            for i in items
        ],
    }


@router.get("/events/{event_id}")
def api_get_event(
    event_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    ev = db.get(WebhookEvent, event_id)
    if not ev:
        raise HTTPException(404, "Event not found")
    try:
        payload = json.loads(ev.payload_json)
    except Exception:
        payload = ev.payload_json
    return {
        "id": ev.id,
        "external_event_id": ev.external_event_id,
        "event_name": ev.event_name,
        "phone": ev.phone,
        "created_at": ev.created_at.isoformat() if ev.created_at else None,
        "payload": payload,
    }


@router.delete("/events/{event_id}")
def api_delete_event(
    event_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    ev = db.get(WebhookEvent, event_id)
    if not ev:
        raise HTTPException(404, "Event not found")
    db.delete(ev)
    db.commit()
    return {"ok": True}


# ── Message logs ────────────────────────────────────────────────────────

@router.get("/messages")
def api_list_messages(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    status: str | None = None,
    phone: str | None = None,
):
    stmt = select(MessageLog)
    if status:
        stmt = stmt.where(MessageLog.status == status)
    if phone:
        stmt = stmt.where(MessageLog.phone.contains(phone))
    stmt = stmt.order_by(MessageLog.created_at.desc())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = db.execute(stmt.offset((page - 1) * page_size).limit(page_size)).scalars().all()
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": [
            {
                "id": i.id,
                "phone": i.phone,
                "template_name": i.template_name,
                "status": i.status,
                "last_status": i.last_status,
                "direction": i.direction,
                "created_at": i.created_at.isoformat() if i.created_at else None,
            }
            for i in items
        ],
    }


@router.get("/messages/{log_id}")
def api_get_message(
    log_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(MessageLog, log_id)
    if not row:
        raise HTTPException(404, "Message log not found")
    return {
        "id": row.id,
        "phone": row.phone,
        "template_name": row.template_name,
        "status": row.status,
        "provider_response": row.provider_response,
        "conversation_event_id": row.conversation_event_id,
        "contact_id": row.contact_id,
        "channel_id": row.channel_id,
        "last_status": row.last_status,
        "last_status_at": row.last_status_at.isoformat() if row.last_status_at else None,
        "direction": row.direction,
        "message_id": row.message_id,
        "error_code": row.error_code,
        "error_reason": row.error_reason,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


@router.delete("/messages/{log_id}")
def api_delete_message(
    log_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(MessageLog, log_id)
    if not row:
        raise HTTPException(404, "Message log not found")
    db.delete(row)
    db.commit()
    return {"ok": True}


# ── Scheduled messages ──────────────────────────────────────────────────

@router.get("/scheduled")
def api_list_scheduled(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    status: str | None = None,
):
    stmt = select(ScheduledMessage)
    if status:
        stmt = stmt.where(ScheduledMessage.status == status)
    stmt = stmt.order_by(ScheduledMessage.run_at.desc())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = db.execute(stmt.offset((page - 1) * page_size).limit(page_size)).scalars().all()
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": [_scheduled_dict(i) for i in items],
    }


def _scheduled_dict(row: ScheduledMessage) -> dict[str, Any]:
    return {
        "id": row.id,
        "external_event_id": row.external_event_id,
        "reservation_number": row.reservation_number,
        "to_phone": row.to_phone,
        "template_name": row.template_name,
        "params_json": row.params_json,
        "run_at": row.run_at.isoformat() if row.run_at else None,
        "status": row.status,
        "attempts": row.attempts,
        "last_error": row.last_error,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.post("/scheduled")
def api_create_scheduled(
    body: ScheduledCreate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = ScheduledMessage(
        to_phone=body.to_phone,
        template_name=body.template_name,
        params_json=body.params_json,
        run_at=body.run_at,
        reservation_number=body.reservation_number,
        external_event_id=body.external_event_id,
        status=body.status,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _scheduled_dict(row)


@router.patch("/scheduled/{job_id}")
def api_update_scheduled(
    job_id: str,
    body: ScheduledUpdate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(ScheduledMessage, job_id)
    if not row:
        raise HTTPException(404, "Scheduled message not found")
    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(row, field, value)
    row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return _scheduled_dict(row)


@router.post("/scheduled/{job_id}/retry")
def api_retry_scheduled(
    job_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(ScheduledMessage, job_id)
    if not row:
        raise HTTPException(404, "Scheduled message not found")
    row.run_at = datetime.utcnow()
    row.status = "pending"
    row.attempts = 0
    row.last_error = None
    row.updated_at = datetime.utcnow()
    db.commit()
    return _scheduled_dict(row)


@router.post("/scheduled/{job_id}/cancel")
def api_cancel_scheduled(
    job_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(ScheduledMessage, job_id)
    if not row:
        raise HTTPException(404, "Scheduled message not found")
    row.status = "canceled"
    row.updated_at = datetime.utcnow()
    db.commit()
    return _scheduled_dict(row)


@router.delete("/scheduled/{job_id}")
def api_delete_scheduled(
    job_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(ScheduledMessage, job_id)
    if not row:
        raise HTTPException(404, "Scheduled message not found")
    db.delete(row)
    db.commit()
    return {"ok": True}


# ── Idempotency locks ───────────────────────────────────────────────────

@router.get("/locks")
def api_list_locks(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
):
    stmt = select(SentNotification).order_by(SentNotification.created_at.desc())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = db.execute(stmt.offset((page - 1) * page_size).limit(page_size)).scalars().all()
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": [
            {
                "id": i.id,
                "reservation_number": i.reservation_number,
                "notification_type": i.notification_type,
                "phone": i.phone,
                "created_at": i.created_at.isoformat() if i.created_at else None,
            }
            for i in items
        ],
    }


@router.delete("/locks/{lock_id}")
def api_delete_lock(
    lock_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(SentNotification, lock_id)
    if not row:
        raise HTTPException(404, "Lock not found")
    db.delete(row)
    db.commit()
    return {"ok": True}


# ── Event mappings ──────────────────────────────────────────────────────

@router.get("/mappings")
def api_list_mappings(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    items = db.execute(
        select(EventTemplateMapping).order_by(EventTemplateMapping.event_name)
    ).scalars().all()
    return {
        "items": [
            {
                "id": i.id,
                "event_name": i.event_name,
                "template_name": i.template_name,
                "enabled": i.enabled,
                "description": i.description,
                "staff_role": i.staff_role,
                "staff_template_name": i.staff_template_name,
                "updated_at": i.updated_at.isoformat() if i.updated_at else None,
            }
            for i in items
        ],
        "known_templates": [t["name"] for t in list_enabled_templates(db)],
    }


@router.post("/mappings")
def api_create_mapping(
    body: MappingCreate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = EventTemplateMapping(
        event_name=body.event_name.strip(),
        template_name=body.template_name.strip(),
        enabled=body.enabled,
        description=body.description,
        staff_role=(body.staff_role or "").strip() or None,
        staff_template_name=(body.staff_template_name or "").strip() or None,
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(400, detail=str(exc)) from exc
    invalidate_mapping_cache()
    db.refresh(row)
    return {"id": row.id}


@router.patch("/mappings/{mapping_id}")
def api_update_mapping(
    mapping_id: str,
    body: MappingUpdate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(EventTemplateMapping, mapping_id)
    if not row:
        raise HTTPException(404, "Mapping not found")
    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(row, field, value)
    row.updated_at = datetime.utcnow()
    db.commit()
    invalidate_mapping_cache()
    return {"ok": True}


@router.delete("/mappings/{mapping_id}")
def api_delete_mapping(
    mapping_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(EventTemplateMapping, mapping_id)
    if not row:
        raise HTTPException(404, "Mapping not found")
    db.delete(row)
    db.commit()
    invalidate_mapping_cache()
    return {"ok": True}


# ── Runtime settings ────────────────────────────────────────────────────

@router.get("/settings")
def api_get_settings(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    return {
        "settings": get_runtime_settings_view(db),
        "roles": NOTIFICATION_ROLES,
    }


@router.patch("/settings")
def api_update_settings(
    body: RuntimeSettingsUpdate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    if body.reminder_before_minutes < 1 or body.reminder_before_minutes > 1440:
        raise HTTPException(400, detail={"message_ar": "دقائق قبل التذكير يجب أن تكون بين 1 و 1440"})
    if body.allowed_late_minutes < 0 or body.allowed_late_minutes > 1440:
        raise HTTPException(400, detail={"message_ar": "دقائق التأخير يجب أن تكون بين 0 و 1440"})

    set_setting(db, SETTING_REMINDER_BEFORE_MINUTES, str(body.reminder_before_minutes))
    set_setting(db, SETTING_ALLOWED_LATE_MINUTES, str(body.allowed_late_minutes))
    invalidate_settings_cache()
    return {"ok": True, "settings": get_runtime_settings_view(db)}


# ── Role recipients ─────────────────────────────────────────────────────

@router.get("/recipients")
def api_list_recipients(
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    return {"roles": NOTIFICATION_ROLES, "recipients": list_recipients_by_role(db)}


@router.post("/recipients")
def api_create_recipient(
    body: RecipientCreate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    if not is_valid_role(body.role):
        raise HTTPException(400, detail={"message_ar": "دور غير صالح"})
    try:
        row = add_recipient(db, body.role, body.phone, body.label, body.enabled)
    except ValueError as exc:
        raise HTTPException(400, detail={"message_ar": str(exc)}) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(400, detail=str(exc)) from exc
    return {"id": row.id}


@router.patch("/recipients/{recipient_id}")
def api_update_recipient(
    recipient_id: str,
    body: RecipientUpdate,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(RoleRecipient, recipient_id)
    if not row:
        raise HTTPException(404, "Recipient not found")
    data = body.model_dump(exclude_unset=True)
    if "label" in data:
        row.label = (data["label"] or "").strip() or None
    if "enabled" in data:
        row.enabled = data["enabled"]
    db.commit()
    invalidate_role_cache()
    return {"ok": True}


@router.delete("/recipients/{recipient_id}")
def api_delete_recipient(
    recipient_id: str,
    _: str = Depends(require_admin_api),
    db: Session = Depends(get_db),
):
    row = db.get(RoleRecipient, recipient_id)
    if not row:
        raise HTTPException(404, "Recipient not found")
    db.delete(row)
    db.commit()
    invalidate_role_cache()
    return {"ok": True}
