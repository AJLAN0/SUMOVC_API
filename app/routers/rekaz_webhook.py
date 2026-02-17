import json
import logging

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models import MessageLog, WebhookEvent
from app.services.hatif import format_provider_response, send_whatsapp_template, send_whatsapp_text
from app.services.rekaz import (
    build_template_parameters,
    build_text_message,
    map_event_to_template,
    normalize_phone,
)

router = APIRouter()
logger = logging.getLogger("app.rekaz_webhook")


def _enforce_rekaz_auth(authorization: str | None, tenant: str | None) -> None:
    expected_auth = f"Basic {settings.REKAZ_BASIC_AUTH}"
    if not authorization or authorization.strip() != expected_auth:
        logger.warning(
            "rekaz_auth_failed_authorization",
            extra={
                "extra": {
                    "received": authorization[:20] + "..." if authorization and len(authorization) > 20 else authorization,
                    "expected_prefix": "Basic ****",
                }
            },
        )
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not tenant or tenant.strip() != settings.REKAZ_TENANT_ID:
        logger.warning(
            "rekaz_auth_failed_tenant",
            extra={
                "extra": {
                    "received_tenant": tenant,
                    "expected_tenant": settings.REKAZ_TENANT_ID,
                }
            },
        )
        raise HTTPException(status_code=401, detail="Unauthorized")
    logger.debug("rekaz_auth_passed")


async def _process_rekaz_webhook(payload: dict, request_id: str) -> None:
    db: Session = SessionLocal()
    try:
        logger.info(
            "rekaz_bg_processing_started",
            extra={"extra": {"request_id": request_id}},
        )

        external_event_id = payload.get("Id") or payload.get("id")
        event_name = payload.get("EventName") or payload.get("eventName")
        data = payload.get("Data") or payload.get("data") or {}
        customer = data.get("customer") or data.get("Customer") or {}

        phone_raw = (
            customer.get("MobileNumber")
            or customer.get("mobileNumber")
            or customer.get("phone")
        )
        phone = normalize_phone(phone_raw)
        customer_name = customer.get("name") or customer.get("Name")
        reservation_number = data.get("number") or data.get("Number")
        product_name = data.get("productName") or data.get("ProductName")
        start_date = data.get("startDate") or data.get("StartDate")

        logger.info(
            "rekaz_payload_extracted",
            extra={
                "extra": {
                    "request_id": request_id,
                    "external_event_id": external_event_id,
                    "event_name": event_name,
                    "phone_raw": phone_raw,
                    "phone_normalized": phone,
                    "customer_name": customer_name,
                    "reservation_number": reservation_number,
                    "product_name": product_name,
                    "start_date": start_date,
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
        status = "failed"
        provider_response = ""
        response_json: dict = {}

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
                event_name, customer_name, reservation_number, product_name, start_date
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
            # --- Template mode ---
            if template_name == "welcome":
                parameters: list[str] = []
                language = "ar"
            else:
                parameters = build_template_parameters(
                    customer_name, reservation_number, product_name, start_date
                )
                language = "ar"

            logger.info(
                "rekaz_sending_template",
                extra={
                    "extra": {
                        "request_id": request_id,
                        "phone": phone,
                        "template": template_name,
                        "language": language,
                        "param_count": len(parameters),
                    }
                },
            )
            try:
                success, response_body, response_json = await send_whatsapp_template(
                    template_name, phone, parameters, language=language
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

        # --- Save MessageLog ---
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


@router.post("/webhooks/rekaz")
async def rekaz_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(default=None, alias="Authorization"),
    tenant: str | None = Header(default=None, alias="__tenant"),
):
    request_id = request.state.request_id

    logger.info(
        "rekaz_webhook_received",
        extra={"extra": {"request_id": request_id}},
    )

    _enforce_rekaz_auth(authorization, tenant)

    logger.info(
        "rekaz_webhook_auth_passed",
        extra={"extra": {"request_id": request_id}},
    )

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
