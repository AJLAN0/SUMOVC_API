import json
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, BackgroundTasks, Header, Request
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models import MessageLog, ScheduledMessage, SentNotification, WebhookEvent
from app.services.hatif import format_provider_response, send_whatsapp_template, send_whatsapp_text
from app.services.rekaz import (
    PayloadKind,
    build_template_parameters,
    build_text_message,
    classify_payload,
    customer_notification_type,
    extract_fields,
    is_reservation_update_event,
    load_previous_reservation_fields,
    map_event_to_template,
    normalize_phone,
    rekaz_start_to_utc,
    reservation_schedule_changed,
    resolve_correlation_id,
    resolve_message_phone,
    resolve_template_language,
    staff_notification_type,
    should_cancel_reminders,
    should_reschedule_reminder_on_update,
    should_schedule_reminder,
    RESERVATION_CONFIRM_TEMPLATE,
)
from app.services.runtime_settings import get_allowed_late_minutes, get_reminder_before_minutes
from app.services.role_recipients import get_phones_for_role

router = APIRouter()
logger = logging.getLogger("app.rekaz_webhook")

# ── Auth guard ──────────────────────────────────────────────────────────

def _enforce_rekaz_auth(authorization: str | None, tenant: str | None) -> None:
    if settings.REKAZ_TENANT_ID and tenant and tenant.strip() != settings.REKAZ_TENANT_ID:
        logger.warning(
            "rekaz_tenant_mismatch",
            extra={"extra": {"received_tenant": tenant, "expected_tenant": settings.REKAZ_TENANT_ID}},
        )
        return

    if authorization:
        expected_auth = f"Basic {settings.REKAZ_BASIC_AUTH}"
        if authorization.strip() != expected_auth:
            logger.warning(
                "rekaz_auth_invalid_but_ignored",
                extra={
                    "extra": {
                        "received": authorization[:20] + "..." if len(authorization) > 20 else authorization,
                        "expected_prefix": "Basic ****",
                    }
                },
            )
            return

    logger.debug("rekaz_guard_checked")


# ── Idempotency guard ────────────────────────────────────────────────────

def _claim_notification_slot(
    reservation_number: str | None,
    notification_type: str,
    phone: str,
    request_id: str,
    db: Session,
) -> bool:
    """
    Try to insert a SentNotification row.  Returns True if the slot was
    claimed (first time), False if this notification was already sent
    (IntegrityError on the unique constraint).
    """
    if not reservation_number:
        logger.debug(
            "idempotency_skipped_no_reservation_number",
            extra={"extra": {"request_id": request_id, "notification_type": notification_type}},
        )
        return True

    lock = SentNotification(
        reservation_number=reservation_number,
        notification_type=notification_type,
        phone=phone,
    )
    db.add(lock)
    try:
        db.flush()
        logger.info(
            "idempotency_lock_inserted",
            extra={
                "extra": {
                    "request_id": request_id,
                    "reservation_number": reservation_number,
                    "notification_type": notification_type,
                    "phone": phone,
                }
            },
        )
        return True
    except IntegrityError:
        db.rollback()
        logger.info(
            "duplicate_suppressed",
            extra={
                "extra": {
                    "request_id": request_id,
                    "reservation_number": reservation_number,
                    "notification_type": notification_type,
                    "phone": phone,
                }
            },
        )
        return False


# ── Staff send helper (role-based) ──────────────────────────────────────

async def _send_staff_notifications(
    fields: dict[str, str],
    event_name: str,
    external_event_id: str | None,
    payload_kind: PayloadKind,
    request_id: str,
    db: Session,
) -> None:
    """Send staff template to phones for the role configured on this event mapping."""
    from app.admin.services import get_staff_notification_for_event

    staff_role, staff_template = get_staff_notification_for_event(db, event_name)
    if not staff_role or not staff_template:
        logger.debug(
            "staff_send_skipped_no_mapping",
            extra={"extra": {"request_id": request_id, "event_name": event_name}},
        )
        return

    staff_phones = get_phones_for_role(db, staff_role)
    if not staff_phones:
        logger.debug(
            "staff_send_skipped_no_phones",
            extra={"extra": {"request_id": request_id, "staff_role": staff_role}},
        )
        return

    reservation_number = fields.get("reservation_number")
    language = "en"
    staff_params = build_template_parameters(
        staff_template,
        fields,
        placeholder=settings.EMPTY_PARAM_PLACEHOLDER,
        db=db,
    )
    notification_type = staff_notification_type(event_name, staff_role, external_event_id, payload_kind)

    for staff_phone in staff_phones:
        if not _claim_notification_slot(reservation_number, notification_type, staff_phone, request_id, db):
            logger.info(
                "staff_confirmed_already_sent",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "staff_phone": staff_phone,
                        "staff_role": staff_role,
                        "reservation_number": reservation_number,
                    }
                },
            )
            continue

        try:
            logger.info(
                "staff_send_started",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "staff_phone": staff_phone,
                        "staff_role": staff_role,
                        "template": staff_template,
                        "param_count": len(staff_params),
                    }
                },
            )
            success, response_body, response_json = await send_whatsapp_template(
                staff_template, staff_phone, staff_params,
                language=language,
            )
            staff_log = MessageLog(
                phone=staff_phone,
                template_name=staff_template,
                status="success" if success else "failed",
                provider_response=format_provider_response(success, response_body),
                conversation_event_id=response_json.get("conversationeventid"),
                contact_id=response_json.get("contactid"),
                channel_id=settings.HATIF_CHANNEL_ID or None,
                last_status=response_json.get("status"),
                error_reason=response_json.get("message"),
            )
            db.add(staff_log)
            db.commit()

            logger.info(
                "staff_send_result",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "staff_phone": staff_phone,
                        "staff_role": staff_role,
                        "success": success,
                        "message_log_id": staff_log.id,
                    }
                },
            )
        except Exception:
            logger.exception(
                "staff_send_failed",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "staff_phone": staff_phone,
                        "staff_role": staff_role,
                    }
                },
            )


# ── Reminder scheduling helper ──────────────────────────────────────────

def _schedule_reminder(
    fields: dict[str, str],
    phone: str,
    external_event_id: str,
    request_id: str,
    db: Session,
) -> None:
    """Create a ScheduledMessage for reservation_reminderrrr if start_dt is in the future."""
    start_iso = fields.get("start_dt_iso", "")
    if not start_iso:
        logger.info("reminder_schedule_skipped_no_start_dt", extra={"extra": {"request_id": request_id}})
        return

    start_utc = rekaz_start_to_utc(start_iso)
    if not start_utc:
        logger.warning(
            "reminder_schedule_skipped_parse_failed",
            extra={"extra": {"request_id": request_id, "start_dt_iso": start_iso}},
        )
        return

    run_at = start_utc - timedelta(minutes=get_reminder_before_minutes(db))
    now_utc = datetime.utcnow()

    if run_at <= now_utc:
        logger.info(
            "reminder_schedule_skipped_past",
            extra={
                "extra": {
                    "request_id": request_id,
                    "run_at": run_at.isoformat(),
                    "now_utc": now_utc.isoformat(),
                }
            },
        )
        return

    reminder_params = build_template_parameters(
        "reservation_reminderrrr",
        fields,
        placeholder=settings.EMPTY_PARAM_PLACEHOLDER,
        db=db,
    )

    job = ScheduledMessage(
        external_event_id=external_event_id,
        reservation_number=fields.get("reservation_number") or None,
        to_phone=phone,
        template_name="reservation_reminderrrr",
        params_json=json.dumps(reminder_params, ensure_ascii=False),
        run_at=run_at,
        status="pending",
    )
    db.add(job)
    try:
        db.commit()
        start_riyadh = start_utc.replace(tzinfo=timezone.utc).astimezone(
            timezone(timedelta(hours=3))
        )
        run_at_riyadh = run_at.replace(tzinfo=timezone.utc).astimezone(
            timezone(timedelta(hours=3))
        )
        logger.info(
            "reminder_scheduled",
            extra={
                "extra": {
                    "request_id": request_id,
                    "job_id": job.id,
                    "to_phone": phone,
                    "run_at_utc": run_at.isoformat(),
                    "run_at_riyadh": run_at_riyadh.strftime("%Y-%m-%d %H:%M"),
                    "start_utc": start_utc.isoformat(),
                    "start_riyadh": start_riyadh.strftime("%Y-%m-%d %H:%M"),
                    "raw_start_iso": start_iso,
                    "reservation_number": fields.get("reservation_number"),
                    "before_minutes": get_reminder_before_minutes(db),
                }
            },
        )
    except IntegrityError:
        db.rollback()
        logger.info(
            "reminder_job_already_exists",
            extra={"extra": {"request_id": request_id, "reservation_number": fields.get("reservation_number")}},
        )


# ── Cancel reminder helper ──────────────────────────────────────────────

def _cancel_reminders(
    fields: dict[str, str],
    request_id: str,
    db: Session,
) -> None:
    """Cancel any pending reminder jobs for the given reservation_number."""
    res_num = fields.get("reservation_number")
    if not res_num:
        logger.debug("cancel_reminders_skipped_no_reservation_number", extra={"extra": {"request_id": request_id}})
        return

    result = db.execute(
        update(ScheduledMessage)
        .where(
            ScheduledMessage.reservation_number == res_num,
            ScheduledMessage.template_name == "reservation_reminderrrr",
            ScheduledMessage.status == "pending",
        )
        .values(status="canceled", updated_at=datetime.utcnow())
    )
    cancelled_count = result.rowcount
    db.commit()

    if cancelled_count:
        logger.info(
            "reminders_cancelled",
            extra={
                "extra": {
                    "request_id": request_id,
                    "reservation_number": res_num,
                    "cancelled_count": cancelled_count,
                }
            },
        )
    else:
        logger.debug(
            "cancel_reminders_none_found",
            extra={"extra": {"request_id": request_id, "reservation_number": res_num}},
        )


# ── Main background processor ──────────────────────────────────────────

async def _process_rekaz_webhook(payload: dict, request_id: str) -> None:
    db: Session = SessionLocal()
    try:
        logger.info(
            "rekaz_bg_processing_started",
            extra={"extra": {"request_id": request_id}},
        )

        external_event_id = payload.get("Id") or payload.get("id")
        event_name = payload.get("EventName") or payload.get("eventName")

        # ── Extract all fields using the centralized helper ──
        fields = extract_fields(payload, event_name)
        if not fields.get("allowed_late_minutes"):
            fields["allowed_late_minutes"] = str(get_allowed_late_minutes(db))

        phone_raw, phone_source = resolve_message_phone(payload, event_name)
        phone = normalize_phone(phone_raw)
        payload_kind = classify_payload(event_name, payload)
        correlation_id = resolve_correlation_id(fields, event_name)

        logger.info(
            "rekaz_payload_extracted",
            extra={
                "extra": {
                    "request_id": request_id,
                    "external_event_id": external_event_id,
                    "event_name": event_name,
                    "payload_kind": payload_kind.value,
                    "phone_raw": phone_raw,
                    "phone_source": phone_source,
                    "phone_normalized": phone,
                    "correlation_id": correlation_id,
                    "fields": fields,
                }
            },
        )

        if not external_event_id or not event_name:
            logger.warning(
                "rekaz_webhook_missing_fields",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "has_event_id": bool(external_event_id),
                        "has_event_name": bool(event_name),
                    }
                },
            )
            return

        # --- Dedupe: insert WebhookEvent ---
        event = WebhookEvent(
            external_event_id=external_event_id,
            event_name=event_name,
            phone=phone,
            payload_json=json.dumps(payload),
        )
        db.add(event)
        try:
            db.commit()
            logger.info(
                "rekaz_webhook_event_saved",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "external_event_id": external_event_id,
                        "event_name": event_name,
                    }
                },
            )
        except IntegrityError:
            db.rollback()
            logger.info(
                "rekaz_webhook_duplicate_skipped",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "external_event_id": external_event_id,
                    }
                },
            )
            return

        # --- Determine send mode ---
        template_name = map_event_to_template(db, event_name)
        language = resolve_template_language(payload, event_name, settings.HATIF_TEMPLATE_LANGUAGE)
        status = "failed"
        provider_response = ""
        response_json: dict = {}
        success = False
        is_duplicate = False
        reservation_number = correlation_id or fields.get("reservation_number")

        schedule_changed = True
        if is_reservation_update_event(event_name):
            previous_fields = load_previous_reservation_fields(
                db, fields.get("reservation_number"), external_event_id
            )
            schedule_changed = reservation_schedule_changed(fields, previous_fields)
            if not schedule_changed:
                logger.info(
                    "reservation_update_skipped_no_schedule_change",
                    extra={
                        "extra": {
                            "request_id": request_id,
                            "external_event_id": external_event_id,
                            "reservation_number": fields.get("reservation_number"),
                            "start_dt_iso": fields.get("start_dt_iso"),
                            "end_dt_iso": fields.get("end_dt_iso"),
                            "reservation_date": fields.get("reservation_date"),
                        }
                    },
                )

        if not phone:
            logger.warning(
                "rekaz_webhook_no_phone",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "external_event_id": external_event_id,
                        "phone_raw": phone_raw,
                        "phone_source": phone_source,
                    }
                },
            )
            provider_response = format_provider_response(False, "missing_phone")

        elif is_reservation_update_event(event_name) and not schedule_changed:
            status = "skipped"
            provider_response = format_provider_response(True, "skipped:no_schedule_change")

        elif settings.HATIF_SEND_MODE == "text":
            text_body = build_text_message(
                event_name,
                fields.get("customer_name"),
                fields.get("reservation_number"),
                fields.get("product_name"),
                fields.get("reservation_date"),
            )
            logger.info(
                "rekaz_sending_text",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "phone": phone,
                        "text_length": len(text_body),
                    }
                },
            )
            try:
                success, response_body, response_json = await send_whatsapp_text(
                    phone, text_body
                )
                status = "success" if success else "failed"
                provider_response = format_provider_response(success, response_body)
                logger.info(
                    "rekaz_text_send_result",
                    extra={
                        "extra": {
                            "request_id": request_id,
                            "phone": phone,
                            "success": success,
                        }
                    },
                )
            except Exception as exc:
                logger.error(
                    "rekaz_text_send_exception",
                    extra={"extra": {"request_id": request_id, "phone": phone, "error": str(exc)}},
                    exc_info=True,
                )
                provider_response = format_provider_response(False, str(exc))

        elif not template_name:
            logger.warning(
                "rekaz_webhook_unsupported_event",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "event_name": event_name,
                    }
                },
            )
            provider_response = format_provider_response(
                False, f"unsupported_event:{event_name}"
            )

        else:
            # --- Template mode (spec-driven) ---

            # ── Idempotency: per-domain rules (confirm once, updates/events by webhook Id) ──
            idempotency_key = correlation_id or external_event_id
            if phone and idempotency_key:
                notif_type = customer_notification_type(event_name, external_event_id, payload_kind)
                if not _claim_notification_slot(idempotency_key, notif_type, phone, request_id, db):
                    is_duplicate = True
                    logger.info(
                        "customer_message_already_sent",
                        extra={
                            "extra": {
                                "request_id": request_id,
                                "idempotency_key": idempotency_key,
                                "phone": phone,
                                "event_name": event_name,
                                "payload_kind": payload_kind.value,
                                "notification_type": notif_type,
                            }
                        },
                    )
                    provider_response = format_provider_response(False, "duplicate_suppressed")
                    status = "duplicate"

            if not is_duplicate:
                parameters = build_template_parameters(
                    template_name,
                    fields,
                    placeholder=settings.EMPTY_PARAM_PLACEHOLDER,
                    db=db,
                )

                # ── Param-count pre-flight check ──
                from app.services.template_catalog import get_spec_for_template

                expected = get_spec_for_template(db, template_name) or None
                if not expected:
                    from app.services.rekaz import TEMPLATE_PARAM_SPECS

                    expected = TEMPLATE_PARAM_SPECS.get(template_name)
                if expected is not None and len(parameters) != len(expected):
                    logger.error(
                        "rekaz_param_count_mismatch",
                        extra={
                            "extra": {
                                "request_id": request_id,
                                "template": template_name,
                                "expected_count": len(expected),
                                "actual_count": len(parameters),
                                "spec_keys": expected,
                                "param_values": parameters,
                            }
                        },
                    )
                    provider_response = format_provider_response(
                        False, f"param_count_mismatch:expected={len(expected)},got={len(parameters)}"
                    )
                else:
                    logger.info(
                        "rekaz_sending_template",
                        extra={
                            "extra": {
                                "request_id": request_id,
                                "phone": phone,
                                "template": template_name,
                                "language": language,
                                "param_count": len(parameters),
                                "parameters": parameters,
                            }
                        },
                    )
                    try:
                        success, response_body, response_json = await send_whatsapp_template(
                            template_name, phone, parameters,
                            language=language,
                        )
                        status = "success" if success else "failed"
                        provider_response = format_provider_response(success, response_body)
                        logger.info(
                            "rekaz_template_send_result",
                            extra={
                                "extra": {
                                    "request_id": request_id,
                                    "phone": phone,
                                    "template": template_name,
                                    "success": success,
                                }
                            },
                        )
                    except Exception as exc:
                        logger.error(
                            "rekaz_template_send_exception",
                            extra={"extra": {"request_id": request_id, "phone": phone, "error": str(exc)}},
                            exc_info=True,
                        )
                        provider_response = format_provider_response(False, str(exc))

            # ── Post-send actions (template mode only) ──

            if success and not is_duplicate and schedule_changed:
                await _send_staff_notifications(
                    fields, event_name, external_event_id, payload_kind, request_id, db
                )

            if should_schedule_reminder(template_name, payload_kind, event_name) and success and not is_duplicate and schedule_changed:
                if phone:
                    if should_reschedule_reminder_on_update(event_name, payload_kind, schedule_changed):
                        _cancel_reminders(fields, request_id, db)
                    _schedule_reminder(fields, phone, external_event_id, request_id, db)

            elif should_schedule_reminder(template_name, payload_kind, event_name) and not success and not is_duplicate:
                logger.warning(
                    "rekaz_skipping_post_actions_send_failed",
                    extra={
                        "extra": {
                            "request_id": request_id,
                            "template": template_name,
                            "reason": "client send failed",
                        }
                    },
                )

            elif should_cancel_reminders(template_name, payload_kind):
                _cancel_reminders(fields, request_id, db)

        # --- Save MessageLog (client) ---
        conversation_event_id = response_json.get("conversationeventid")
        contact_id = response_json.get("contactid")

        message_log = MessageLog(
            phone=phone,
            template_name=template_name,
            status=status,
            provider_response=provider_response,
            conversation_event_id=conversation_event_id,
            contact_id=contact_id,
            channel_id=settings.HATIF_CHANNEL_ID or None,
            last_status=response_json.get("status"),
            error_reason=response_json.get("message"),
        )
        db.add(message_log)
        db.commit()

        logger.info(
            "rekaz_message_log_saved",
            extra={
                "extra": {
                    "request_id": request_id,
                    "message_log_id": message_log.id,
                    "event_name": event_name,
                    "external_event_id": external_event_id,
                    "phone": phone,
                    "send_status": status,
                    "send_mode": settings.HATIF_SEND_MODE,
                    "is_duplicate": is_duplicate,
                    "conversation_event_id": conversation_event_id,
                    "contact_id": contact_id,
                }
            },
        )

        logger.info(
            "rekaz_bg_processing_completed",
            extra={"extra": {"request_id": request_id, "status": status}},
        )
    except Exception:
        logger.error(
            "rekaz_bg_processing_unhandled_error",
            extra={"extra": {"request_id": request_id}},
            exc_info=True,
        )
    finally:
        db.close()
        logger.debug("rekaz_bg_db_session_closed", extra={"extra": {"request_id": request_id}})


# ── Route ───────────────────────────────────────────────────────────────

@router.post("/webhooks/rekaz")
async def rekaz_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(default=None, alias="Authorization"),
    tenant: str | None = Header(default=None, alias="__tenant"),
):
    request_id = request.state.request_id

    logger.info("rekaz_webhook_received", extra={"extra": {"request_id": request_id}})

    _enforce_rekaz_auth(authorization, tenant)

    logger.info("rekaz_webhook_guard_checked", extra={"extra": {"request_id": request_id}})

    try:
        body = await request.body()
        payload = json.loads(body.decode("utf-8"))
        logger.info(
            "rekaz_webhook_payload_parsed",
            extra={
                "extra": {
                    "request_id": request_id,
                    "body_size": len(body),
                    "event_id": payload.get("Id") or payload.get("id"),
                    "event_name": payload.get("EventName") or payload.get("eventName"),
                }
            },
        )
    except Exception:
        logger.warning(
            "rekaz_webhook_invalid_json",
            extra={"extra": {"request_id": request_id}},
            exc_info=True,
        )
        return {"status": "ok"}

    background_tasks.add_task(_process_rekaz_webhook, payload, request_id)

    logger.info(
        "rekaz_webhook_accepted_bg_enqueued",
        extra={"extra": {"request_id": request_id}},
    )

    return {"status": "ok"}
