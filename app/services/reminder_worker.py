import asyncio
import json
import logging
from datetime import datetime

from sqlalchemy import select

from app.config import settings
from app.database import SessionLocal
from app.models import MessageLog, ScheduledMessage
from app.services.hatif import format_provider_response, send_whatsapp_template

logger = logging.getLogger("app.reminder_worker")

POLL_SECONDS = 5
BATCH_SIZE = 50
MAX_ATTEMPTS = 5


async def reminder_worker_loop() -> None:
    """Async loop that polls scheduled_messages every POLL_SECONDS and sends due reminders."""
    logger.info(
        "reminder_worker_started",
        extra={"extra": {"poll_seconds": POLL_SECONDS, "batch_size": BATCH_SIZE, "max_attempts": MAX_ATTEMPTS}},
    )
    while True:
        try:
            await _tick()
        except Exception:
            logger.exception("reminder_worker_tick_failed")
        await asyncio.sleep(POLL_SECONDS)


async def _tick() -> None:
    now = datetime.utcnow()
    db = SessionLocal()
    try:
        jobs = (
            db.execute(
                select(ScheduledMessage)
                .where(
                    ScheduledMessage.status == "pending",
                    ScheduledMessage.run_at <= now,
                    ScheduledMessage.attempts < MAX_ATTEMPTS,
                )
                .order_by(ScheduledMessage.run_at.asc())
                .limit(BATCH_SIZE)
            )
            .scalars()
            .all()
        )

        if not jobs:
            return

        logger.info(
            "reminder_worker_processing_batch",
            extra={"extra": {"job_count": len(jobs), "now": now.isoformat()}},
        )

        for job in jobs:
            job.attempts += 1
            job.updated_at = datetime.utcnow()

            try:
                params = json.loads(job.params_json or "[]")

                logger.info(
                    "reminder_sending",
                    extra={
                        "extra": {
                            "job_id": job.id,
                            "to_phone": job.to_phone,
                            "template": job.template_name,
                            "attempt": job.attempts,
                            "reservation_number": job.reservation_number,
                            "params": params,
                        }
                    },
                )

                success, response_body, response_json = await send_whatsapp_template(
                    job.template_name,
                    job.to_phone,
                    params,
                    language=settings.HATIF_TEMPLATE_LANGUAGE,
                )

                if success:
                    job.status = "sent"
                    job.last_error = None
                    logger.info(
                        "reminder_sent_ok",
                        extra={"extra": {"job_id": job.id, "to_phone": job.to_phone}},
                    )
                else:
                    # Keep pending so retries happen (up to MAX_ATTEMPTS)
                    job.last_error = (response_body or "")[:500]
                    if job.attempts >= MAX_ATTEMPTS:
                        job.status = "failed"
                    logger.warning(
                        "reminder_send_failed",
                        extra={
                            "extra": {
                                "job_id": job.id,
                                "to_phone": job.to_phone,
                                "attempt": job.attempts,
                                "max_attempts": MAX_ATTEMPTS,
                                "error": job.last_error,
                            }
                        },
                    )

                # Save a MessageLog for the reminder send
                msg_log = MessageLog(
                    phone=job.to_phone,
                    template_name=job.template_name,
                    status="success" if success else "failed",
                    provider_response=format_provider_response(success, response_body),
                    conversation_event_id=response_json.get("conversationeventid"),
                    contact_id=response_json.get("contactid"),
                    channel_id=settings.HATIF_CHANNEL_ID or None,
                    last_status=response_json.get("status"),
                    error_reason=response_json.get("message"),
                )
                db.add(msg_log)

            except Exception as exc:
                job.last_error = str(exc)[:500]
                if job.attempts >= MAX_ATTEMPTS:
                    job.status = "failed"
                logger.exception(
                    "reminder_send_exception",
                    extra={"extra": {"job_id": job.id, "to_phone": job.to_phone, "attempt": job.attempts}},
                )

            db.add(job)

        db.commit()
        logger.info("reminder_worker_batch_committed", extra={"extra": {"job_count": len(jobs)}})

    finally:
        db.close()
