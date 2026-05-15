import json
from datetime import datetime
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.admin.auth import (
    authenticate,
    clear_login_failures,
    is_authenticated,
    is_login_rate_limited,
    login_session,
    logout_session,
    record_login_failure,
    require_admin_page,
)
from app.admin.deps import templates
from app.admin.services import get_dashboard_stats, probe_database
from app.config import settings
from app.database import get_db
from app.models import (
    EventTemplateMapping,
    MessageLog,
    ScheduledMessage,
    SentNotification,
    WebhookEvent,
)
from app.services.rekaz import TEMPLATE_PARAM_SPECS

router = APIRouter(tags=["admin-pages"])


def _auth_or_redirect(auth: str | RedirectResponse) -> RedirectResponse | None:
    if isinstance(auth, RedirectResponse):
        return auth
    return None


@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    if is_authenticated(request):
        return RedirectResponse("/dashboard", status_code=302)
    return RedirectResponse("/login", status_code=302)


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "configured": settings.admin_configured(),
            "error": None,
        },
    )


@router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    if not settings.admin_configured():
        return templates.TemplateResponse(
            request,
            "login.html",
            {"configured": False, "error": "Admin is not configured on the server."},
            status_code=503,
        )
    if is_login_rate_limited(request):
        return templates.TemplateResponse(
            request,
            "login.html",
            {"configured": True, "error": "Too many failed attempts. Try again in 15 minutes."},
            status_code=429,
        )
    if not authenticate(email, password):
        record_login_failure(request)
        return templates.TemplateResponse(
            request,
            "login.html",
            {"configured": True, "error": "Invalid email or password."},
            status_code=401,
        )
    clear_login_failures(request)
    login_session(request, email)
    return RedirectResponse("/dashboard", status_code=302)


@router.post("/logout")
async def logout(request: Request):
    logout_session(request)
    return RedirectResponse("/login", status_code=302)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    stats = get_dashboard_stats(db)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"stats": stats, "active": "dashboard"},
    )


@router.get("/dashboard/events", response_class=HTMLResponse)
async def events_page(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    page: int = 1,
    event_name: str | None = None,
    phone: str | None = None,
    q: str | None = None,
):
    if redir := _auth_or_redirect(auth):
        return redir
    page_size = 25
    stmt = select(WebhookEvent)
    if event_name:
        stmt = stmt.where(WebhookEvent.event_name == event_name)
    if phone:
        stmt = stmt.where(WebhookEvent.phone.contains(phone))
    if q:
        stmt = stmt.where(
            or_(WebhookEvent.payload_json.contains(q), WebhookEvent.external_event_id.contains(q))
        )
    stmt = stmt.order_by(WebhookEvent.created_at.desc())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = db.execute(stmt.offset((page - 1) * page_size).limit(page_size)).scalars().all()
    return templates.TemplateResponse(
        request,
        "events.html",
        {
            "active": "events",
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
            "event_name": event_name or "",
            "phone": phone or "",
            "q": q or "",
        },
    )


@router.get("/dashboard/events/{event_id}", response_class=HTMLResponse)
async def event_detail_page(
    request: Request,
    event_id: str,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    ev = db.get(WebhookEvent, event_id)
    if not ev:
        return RedirectResponse("/dashboard/events", status_code=302)
    try:
        payload_pretty = json.dumps(json.loads(ev.payload_json), ensure_ascii=False, indent=2)
    except Exception:
        payload_pretty = ev.payload_json
    return templates.TemplateResponse(
        request,
        "event_detail.html",
        {"active": "events", "event": ev, "payload_pretty": payload_pretty},
    )


@router.get("/dashboard/messages", response_class=HTMLResponse)
async def messages_page(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    page: int = 1,
    status: str | None = None,
    phone: str | None = None,
):
    if redir := _auth_or_redirect(auth):
        return redir
    page_size = 25
    stmt = select(MessageLog)
    if status:
        stmt = stmt.where(MessageLog.status == status)
    if phone:
        stmt = stmt.where(MessageLog.phone.contains(phone))
    stmt = stmt.order_by(MessageLog.created_at.desc())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = db.execute(stmt.offset((page - 1) * page_size).limit(page_size)).scalars().all()
    return templates.TemplateResponse(
        request,
        "messages.html",
        {
            "active": "messages",
            "items": items,
            "page": page,
            "total": total,
            "status": status or "",
            "phone": phone or "",
        },
    )


@router.get("/dashboard/messages/{log_id}", response_class=HTMLResponse)
async def message_detail_page(
    request: Request,
    log_id: str,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    row = db.get(MessageLog, log_id)
    if not row:
        return RedirectResponse("/dashboard/messages", status_code=302)
    return templates.TemplateResponse(
        request,
        "message_detail.html",
        {"active": "messages", "msg": row},
    )


@router.get("/dashboard/scheduled", response_class=HTMLResponse)
async def scheduled_page(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    page: int = 1,
    status: str | None = None,
):
    if redir := _auth_or_redirect(auth):
        return redir
    page_size = 25
    stmt = select(ScheduledMessage)
    if status:
        stmt = stmt.where(ScheduledMessage.status == status)
    stmt = stmt.order_by(ScheduledMessage.run_at.desc())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = db.execute(stmt.offset((page - 1) * page_size).limit(page_size)).scalars().all()
    return templates.TemplateResponse(
        request,
        "scheduled.html",
        {
            "active": "scheduled",
            "items": items,
            "page": page,
            "total": total,
            "status": status or "",
            "templates": list(TEMPLATE_PARAM_SPECS.keys()),
        },
    )


@router.post("/dashboard/scheduled/create", response_class=HTMLResponse)
async def scheduled_create(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    to_phone: str = Form(...),
    template_name: str = Form(...),
    params_json: str = Form("[]"),
    run_at: str = Form(...),
    reservation_number: str = Form(""),
    status: str = Form("pending"),
):
    if redir := _auth_or_redirect(auth):
        return redir
    try:
        run_at_dt = datetime.fromisoformat(run_at.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        run_at_dt = datetime.utcnow()
    row = ScheduledMessage(
        to_phone=to_phone.strip(),
        template_name=template_name.strip(),
        params_json=params_json.strip() or "[]",
        run_at=run_at_dt,
        reservation_number=reservation_number.strip() or None,
        status=status,
    )
    db.add(row)
    db.commit()
    return RedirectResponse("/dashboard/scheduled", status_code=302)


@router.get("/dashboard/locks", response_class=HTMLResponse)
async def locks_page(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    page: int = 1,
):
    if redir := _auth_or_redirect(auth):
        return redir
    page_size = 25
    stmt = select(SentNotification).order_by(SentNotification.created_at.desc())
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = db.execute(stmt.offset((page - 1) * page_size).limit(page_size)).scalars().all()
    return templates.TemplateResponse(
        request,
        "locks.html",
        {"active": "locks", "items": items, "page": page, "total": total},
    )


@router.get("/dashboard/mappings", response_class=HTMLResponse)
async def mappings_page(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    items = db.execute(
        select(EventTemplateMapping).order_by(EventTemplateMapping.event_name)
    ).scalars().all()
    return templates.TemplateResponse(
        request,
        "mappings.html",
        {
            "active": "mappings",
            "items": items,
            "known_templates": list(TEMPLATE_PARAM_SPECS.keys()),
        },
    )


@router.post("/dashboard/mappings/create", response_class=HTMLResponse)
async def mapping_create(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    event_name: str = Form(...),
    template_name: str = Form(...),
    enabled: str = Form("on"),
    description: str = Form(""),
):
    if redir := _auth_or_redirect(auth):
        return redir
    from app.admin.services import invalidate_mapping_cache

    row = EventTemplateMapping(
        event_name=event_name.strip(),
        template_name=template_name.strip(),
        enabled=enabled == "on",
        description=description.strip() or None,
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    invalidate_mapping_cache()
    return RedirectResponse("/dashboard/mappings", status_code=302)


@router.post("/dashboard/mappings/{mapping_id}/toggle", response_class=HTMLResponse)
async def mapping_toggle(
    request: Request,
    mapping_id: str,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    from app.admin.services import invalidate_mapping_cache

    row = db.get(EventTemplateMapping, mapping_id)
    if row:
        row.enabled = not row.enabled
        row.updated_at = datetime.utcnow()
        db.commit()
        invalidate_mapping_cache()
    return RedirectResponse("/dashboard/mappings", status_code=302)


@router.get("/dashboard/system", response_class=HTMLResponse)
async def system_page(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    mapping_count = db.scalar(select(func.count()).select_from(EventTemplateMapping)) or 0
    return templates.TemplateResponse(
        request,
        "system.html",
        {
            "active": "system",
            "settings": settings.admin_settings_masked(),
            "mapping_count": mapping_count,
            "db_probe": probe_database(),
        },
    )
