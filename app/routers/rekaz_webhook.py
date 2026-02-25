import json
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, BackgroundTasks, Header, Request
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models import MessageLog, ScheduledMessage, WebhookEvent
from app.services.hatif import format_provider_response, send_whatsapp_template, send_whatsapp_text
from app.services.rekaz import (
    TEMPLATE_PARAM_SPECS,
    _parse_dt,
    build_template_parameters,
    build_text_message,
    extract_fields,
    map_event_to_template,
    normalize_phone,
)

router = APIRouter()
logger = logging.getLogger("app.rekaz_webhook")

# Riyadh offset (UTC+3) — used when Rekaz sends naive (no-tz) datetimes
_RIYADH = timezone(timedelta(hours=3))


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


# ── Admin send helper ───────────────────────────────────────────────────

async def _send_admin_notifications(
    fields: dict[str, str],
    request_id: str,
    db: Session,
) -> None:
    """Send admin_reservation_confirmed to each configured admin phone."""
    admin_phones = settings.admin_numbers()
    if not admin_phones:
        logger.debug("admin_send_skipped_no_admin_numbers", extra={"extra": {"request_id": request_id}})
        return

    admin_template = "admin_reservation_confirmed"
    language = settings.HATIF_TEMPLATE_LANGUAGE
    admin_params = build_template_parameters(
        admin_template, fields, placeholder=settings.EMPTY_PARAM_PLACEHOLDER,
    )

    for admin_phone in admin_phones:
        try:
            logger.info(
                "admin_send_started",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "admin_phone": admin_phone,
                        "template": admin_template,
                        "param_count": len(admin_params),
                    }
                },
            )
            success, response_body, response_json = await send_whatsapp_template(
                admin_template, admin_phone, admin_params,
                language=language,
            )
            # Save MessageLog for admin
            admin_log = MessageLog(
                phone=admin_phone,
                template_name=admin_template,
                status="success" if success else "failed",
                provider_response=format_provider_response(success, response_body),
                conversation_event_id=response_json.get("conversationeventid"),
                contact_id=response_json.get("contactid"),
                channel_id=settings.HATIF_CHANNEL_ID or None,
                last_status=response_json.get("status"),
                error_reason=response_json.get("message"),
            )
            db.add(admin_log)
            db.commit()

            logger.info(
                "admin_send_result",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "admin_phone": admin_phone,
                        "success": success,
                        "message_log_id": admin_log.id,
                    }
                },
            )
        except Exception:
            logger.exception(
                "admin_send_failed",
                extra={"extra": {"request_id": request_id, "admin_phone": admin_phone}},
            )


# ── Reminder scheduling helper ──────────────────────────────────────────

def _schedule_reminder(
    fields: dict[str, str],
    phone: str,
    external_event_id: str,
    request_id: str,
    db: Session,
) -> None:
    """Create a ScheduledMessage for reservation_reminder if start_dt is in the future."""
    start_iso = fields.get("start_dt_iso", "")
    if not start_iso:
        logger.info("reminder_schedule_skipped_no_start_dt", extra={"extra": {"request_id": request_id}})
        return

    start_dt = _parse_dt(start_iso)
    if not start_dt:
        logger.warning(
            "reminder_schedule_skipped_parse_failed",
            extra={"extra": {"request_id": request_id, "start_dt_iso": start_iso}},
        )
        return

    # ── Timezone-aware UTC conversion ──
    # If Rekaz sent a tz-aware datetime (Z / +00:00), convert to UTC.
    # If naive (no tz info), assume Riyadh (Asia/Riyadh = UTC+3).
    if start_dt.tzinfo is not None:
        start_utc = start_dt.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        # Treat as Riyadh local → convert to UTC by subtracting 3 hours
        start_utc = (start_dt.replace(tzinfo=_RIYADH)
                     .astimezone(timezone.utc)
                     .replace(tzinfo=None))
        logger.info(
            "reminder_naive_dt_assumed_riyadh",
            extra={
                "extra": {
                    "request_id": request_id,
                    "raw_iso": start_iso,
                    "assumed_riyadh": start_dt.isoformat(),
                    "converted_utc": start_utc.isoformat(),
                }
            },
        )

    run_at = start_utc - timedelta(minutes=settings.REMINDER_BEFORE_MINUTES)
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

    # Build reminder params with settings values
    reminder_fields = dict(fields)
    reminder_fields["reservation_after_minutes"] = str(settings.REMINDER_BEFORE_MINUTES)
    reminder_fields["allowed_late_minutes"] = str(settings.ALLOWED_LATE_MINUTES)
    reminder_params = build_template_parameters(
        "reservation_reminder", reminder_fields,
        placeholder=settings.EMPTY_PARAM_PLACEHOLDER,
    )

    job = ScheduledMessage(
        external_event_id=external_event_id,
        reservation_number=fields.get("reservation_number") or None,
        to_phone=phone,
        template_name="reservation_reminder",
        params_json=json.dumps(reminder_params, ensure_ascii=False),
        run_at=run_at,
        status="pending",
    )
    db.add(job)
    try:
        db.commit()
        logger.info(
            "reminder_scheduled",
            extra={
                "extra": {
                    "request_id": request_id,
                    "job_id": job.id,
                    "to_phone": phone,
                    "run_at": run_at.isoformat(),
                    "reservation_number": fields.get("reservation_number"),
                    "before_minutes": settings.REMINDER_BEFORE_MINUTES,
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
            ScheduledMessage.template_name == "reservation_reminder",
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
        fields = extract_fields(payload)

        data = payload.get("Data") or payload.get("data") or {}
        customer = data.get("customer") or data.get("Customer") or {}
        phone_raw = (
            customer.get("MobileNumber")
            or customer.get("mobileNumber")
            or customer.get("phone")
        )
        phone = normalize_phone(phone_raw)

        logger.info(
            "rekaz_payload_extracted",
            extra={
                "extra": {
                    "request_id": request_id,
                    "external_event_id": external_event_id,
                    "event_name": event_name,
                    "phone_raw": phone_raw,
                    "phone_normalized": phone,
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
        template_name = map_event_to_template(event_name)
        language = settings.HATIF_TEMPLATE_LANGUAGE
        status = "failed"
        provider_response = ""
        response_json: dict = {}
        success = False

        if not phone:
            logger.warning(
                "rekaz_webhook_no_phone",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "external_event_id": external_event_id,
                        "phone_raw": phone_raw,
                    }
                },
            )
            provider_response = format_provider_response(False, "missing_phone")

        elif settings.HATIF_SEND_MODE == "text":
            # --- Text mode ---
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
            parameters = build_template_parameters(
                template_name, fields, placeholder=settings.EMPTY_PARAM_PLACEHOLDER,
            )

            # ── Param-count pre-flight check ──
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
            # Only schedule reminder & notify admin when client send SUCCEEDED.

            if template_name == "reservation_confirmed" and success:
                # 1) Send admin notification
                await _send_admin_notifications(fields, request_id, db)
                # 2) Schedule reminder
                if phone:
                    _schedule_reminder(fields, phone, external_event_id, request_id, db)

            elif template_name == "reservation_confirmed" and not success:
                logger.warning(
                    "rekaz_skipping_post_actions_send_failed",
                    extra={
                        "extra": {
                            "request_id": request_id,
                            "template": template_name,
                            "reason": "client send failed — no admin notification, no reminder scheduled",
                        }
                    },
                )

            elif template_name == "reservation_cancelled":
                # Cancel any pending reminders for this reservation
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
