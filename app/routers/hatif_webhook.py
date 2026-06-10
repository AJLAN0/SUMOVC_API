import logging

from fastapi import APIRouter, Depends, Header, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.hatif_webhook import (
    process_call_webhook,
    process_whatsapp_webhook,
    verify_hatif_webhook,
)

router = APIRouter()
logger = logging.getLogger("app.hatif_webhook")


@router.post("/webhooks/hatif/whatsapp")
async def hatif_whatsapp_webhook(
    request: Request,
    db: Session = Depends(get_db),
    signature: str | None = Header(default=None, alias="X-Voxa-Signature"),
):
    body = await request.body()
    body_utf8 = body.decode("utf-8", errors="replace")
    request_id = request.state.request_id

    logger.info(
        "hatif_webhook_received",
        extra={
            "extra": {
                "request_id": request_id,
                "kind": "whatsapp",
                "body_size": len(body),
                "has_signature": signature is not None,
            }
        },
    )

    verify_hatif_webhook(body_utf8, signature, request_id)
    return process_whatsapp_webhook(db, body_utf8, request_id)


@router.post("/webhooks/hatif/call")
async def hatif_call_webhook(
    request: Request,
    db: Session = Depends(get_db),
    signature: str | None = Header(default=None, alias="X-Voxa-Signature"),
):
    body = await request.body()
    body_utf8 = body.decode("utf-8", errors="replace")
    request_id = request.state.request_id

    logger.info(
        "hatif_webhook_received",
        extra={
            "extra": {
                "request_id": request_id,
                "kind": "call",
                "body_size": len(body),
                "has_signature": signature is not None,
            }
        },
    )

    verify_hatif_webhook(body_utf8, signature, request_id)
    return process_call_webhook(db, body_utf8, request_id)
