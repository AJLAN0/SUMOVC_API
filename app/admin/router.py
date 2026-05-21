import json
from datetime import datetime
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
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
from app.admin.deps import render_admin
from app.admin.errors import explain_error, validate_phone
from app.admin.flash import flash_error, flash_success, flash_warning
from app.admin.services import get_dashboard_stats, probe_database
from app.config import settings
from app.database import get_db
from app.models import (
    EventTemplateMapping,
    MessageLog,
    ScheduledMessage,
    SentNotification,
    WebhookEvent,
    WhatsAppTemplate,
)
from app.services.template_catalog import (
    PARAM_LABELS_AR,
    _parse_param_keys,
    build_params_from_form,
    get_spec_for_template,
    invalidate_template_cache,
    list_all_templates,
    list_enabled_templates,
    param_keys_to_json,
)

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
    return render_admin(
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
        return render_admin(
            request,
            "login.html",
            {"configured": False, "error": "لوحة التحكم غير مُعدّة على الخادم."},
            status_code=503,
        )
    if is_login_rate_limited(request):
        return render_admin(
            request,
            "login.html",
            {"configured": True, "error": "محاولات كثيرة فاشلة. حاول بعد ١٥ دقيقة."},
            status_code=429,
        )
    if not authenticate(email, password):
        record_login_failure(request)
        return render_admin(
            request,
            "login.html",
            {"configured": True, "error": "البريد الإلكتروني أو كلمة المرور غير صحيحة."},
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
    return render_admin(
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
    return render_admin(
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
    return render_admin(
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
    return render_admin(
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
    return render_admin(
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
    return render_admin(
        request,
        "scheduled.html",
        {
            "active": "scheduled",
            "items": items,
            "page": page,
            "total": total,
            "status": status or "",
            "templates": list_enabled_templates(db),
        },
    )


@router.get("/dashboard/partials/template-fields", response_class=HTMLResponse)
async def template_fields_partial(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    template_name: str = "",
):
    if redir := _auth_or_redirect(auth):
        return redir
    name = (template_name or request.query_params.get("template_name") or "").strip()
    spec = get_spec_for_template(db, name) if name else []
    return render_admin(
        request,
        "partials/template_param_fields.html",
        {
            "template_name": name,
            "spec": spec,
            "param_labels": PARAM_LABELS_AR,
            "default_placeholder": "-",
        },
    )


@router.post("/dashboard/scheduled/create", response_class=HTMLResponse)
async def scheduled_create(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    form = await request.form()
    to_phone = str(form.get("to_phone", "")).strip()
    template_name = str(form.get("template_name", "")).strip()
    run_at = str(form.get("run_at", ""))
    reservation_number = str(form.get("reservation_number", "")).strip()
    status = str(form.get("status", "pending") or "pending")

    if phone_err := validate_phone(to_phone):
        flash_error(request, phone_err, hint="مثال: 966583771046")
        return RedirectResponse("/dashboard/scheduled", status_code=302)
    if not template_name:
        flash_error(request, "اختر قالب واتساب.", hint="أضف قوالب من قسم «قوالب واتساب» إن لم تظهر.")
        return RedirectResponse("/dashboard/scheduled", status_code=302)
    spec = get_spec_for_template(db, template_name)
    if not spec:
        flash_warning(
            request,
            f"القالب «{template_name}» بدون متغيرات معرّفة.",
            hint="عرّف المتغيرات في قسم القوالب ثم أعد الجدولة.",
        )
    if not run_at.strip():
        flash_error(request, "حدد وقت الإرسال.")
        return RedirectResponse("/dashboard/scheduled", status_code=302)

    params = build_params_from_form(spec, dict(form))
    params_json = json.dumps(params, ensure_ascii=False)
    try:
        run_at_dt = datetime.fromisoformat(run_at.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        flash_error(request, "صيغة وقت الإرسال غير صحيحة.", hint="استخدم منتقي التاريخ والوقت في النموذج.")
        return RedirectResponse("/dashboard/scheduled", status_code=302)
    if run_at_dt < datetime.utcnow():
        flash_warning(request, "وقت الإرسال في الماضي — سيُرسل التذكير فوراً عند أول دورة للنظام.")

    row = ScheduledMessage(
        to_phone=to_phone,
        template_name=template_name,
        params_json=params_json,
        run_at=run_at_dt,
        reservation_number=reservation_number or None,
        status=status,
    )
    db.add(row)
    db.commit()
    flash_success(request, "تم جدولة التذكير بنجاح.", hint=f"الإرسال المتوقع: {run_at_dt.strftime('%Y-%m-%d %H:%M')} UTC")
    return RedirectResponse("/dashboard/scheduled", status_code=302)


@router.get("/dashboard/templates", response_class=HTMLResponse)
async def templates_page(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    return render_admin(
        request,
        "templates.html",
        {
            "active": "templates",
            "items": list_all_templates(db),
            "param_labels": PARAM_LABELS_AR,
        },
    )


@router.post("/dashboard/templates/create", response_class=HTMLResponse)
async def templates_create(
    request: Request,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    name: str = Form(...),
    param_keys_text: str = Form(...),
    title_ar: str = Form(""),
    description: str = Form(""),
    enabled: str = Form("on"),
):
    if redir := _auth_or_redirect(auth):
        return redir
    keys = _parse_param_keys(param_keys_text)
    if not keys:
        flash_error(request, "أضف متغيراً واحداً على الأقل.", hint="سطر لكل متغير: customer_name")
        return RedirectResponse("/dashboard/templates", status_code=302)
    row = WhatsAppTemplate(
        name=name.strip(),
        title_ar=title_ar.strip() or None,
        description=description.strip() or None,
        param_keys_json=param_keys_to_json(keys),
        enabled=enabled == "on",
        updated_at=datetime.utcnow(),
    )
    try:
        db.add(row)
        db.commit()
        invalidate_template_cache()
        flash_success(request, f"تمت إضافة القالب «{row.name}».")
    except IntegrityError as exc:
        db.rollback()
        ex = explain_error(str(exc))
        flash_error(request, ex["message"] or "اسم القالب مستخدم مسبقاً.", hint=ex["hint"])
    return RedirectResponse("/dashboard/templates", status_code=302)


@router.get("/dashboard/templates/{template_id}/edit", response_class=HTMLResponse)
async def templates_edit_page(
    request: Request,
    template_id: str,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
):
    if redir := _auth_or_redirect(auth):
        return redir
    tpl = db.get(WhatsAppTemplate, template_id)
    if not tpl:
        return RedirectResponse("/dashboard/templates", status_code=302)
    from app.services.template_catalog import param_keys_from_json

    keys = param_keys_from_json(tpl.param_keys_json)
    return render_admin(
        request,
        "templates_edit.html",
        {
            "active": "templates",
            "tpl": tpl,
            "param_keys_text": "\n".join(keys),
        },
    )


@router.post("/dashboard/templates/{template_id}/edit", response_class=HTMLResponse)
async def templates_edit_submit(
    request: Request,
    template_id: str,
    auth: str | RedirectResponse = Depends(require_admin_page),
    db: Session = Depends(get_db),
    title_ar: str = Form(""),
    param_keys_text: str = Form(...),
    description: str = Form(""),
    enabled: str = Form(""),
):
    if redir := _auth_or_redirect(auth):
        return redir
    tpl = db.get(WhatsAppTemplate, template_id)
    if not tpl:
        flash_error(request, "القالب غير موجود.", hint="حدّث الصفحة.")
        return RedirectResponse("/dashboard/templates", status_code=302)
    keys = _parse_param_keys(param_keys_text)
    if not keys:
        flash_error(request, "أضف متغيراً واحداً على الأقل.")
        return RedirectResponse(f"/dashboard/templates/{template_id}/edit", status_code=302)
    tpl.title_ar = title_ar.strip() or None
    tpl.description = description.strip() or None
    tpl.param_keys_json = param_keys_to_json(keys)
    tpl.enabled = enabled == "on"
    tpl.updated_at = datetime.utcnow()
    db.commit()
    invalidate_template_cache()
    flash_success(request, "تم حفظ تعديلات القالب.")
    return RedirectResponse("/dashboard/templates", status_code=302)


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
    return render_admin(
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
    return render_admin(
        request,
        "mappings.html",
        {
            "active": "mappings",
            "items": items,
            "template_list": list_enabled_templates(db),
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

    if not event_name.strip() or not template_name.strip():
        flash_error(request, "أدخل اسم الحدث واختر القالب.")
        return RedirectResponse("/dashboard/mappings", status_code=302)
    row = EventTemplateMapping(
        event_name=event_name.strip(),
        template_name=template_name.strip(),
        enabled=enabled == "on",
        description=description.strip() or None,
        updated_at=datetime.utcnow(),
    )
    try:
        db.add(row)
        db.commit()
        invalidate_mapping_cache()
        flash_success(
            request,
            f"تم ربط «{row.event_name}» بالقالب «{row.template_name}».",
            hint="فعّل الربط إن كان الحدث لا يزال لا يرسل رسائل.",
        )
    except IntegrityError as exc:
        db.rollback()
        ex = explain_error(str(exc))
        flash_error(request, ex["message"] or "ربط مكرر لهذا الحدث.", hint=ex["hint"])
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
    from app.admin.i18n import SETTINGS_LABELS_AR

    return render_admin(
        request,
        "system.html",
        {
            "active": "system",
            "settings": settings.admin_settings_masked(),
            "settings_labels": SETTINGS_LABELS_AR,
            "mapping_count": mapping_count,
            "db_probe": probe_database(),
        },
    )
