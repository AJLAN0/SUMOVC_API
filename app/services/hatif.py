import json
import logging
import time

import httpx

from app.config import settings
from app.services.token_cache import TokenCache

logger = logging.getLogger("app.hatif")

_token_cache = TokenCache()


def _normalize_keys(d: dict) -> dict:
    """Return a new dict with all keys lower-cased (single level)."""
    return {k.lower(): v for k, v in d.items()}


async def _fetch_token() -> tuple[str, int]:
    token_url = f"{settings.HATIF_BASE_URL.rstrip('/')}/connect/token"
    logger.info(
        "hatif_token_request",
        extra={"extra": {"url": token_url, "client_id": settings.HATIF_CLIENT_ID}},
    )
    data = {
        "client_id": settings.HATIF_CLIENT_ID,
        "client_secret": settings.HATIF_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": settings.HATIF_SCOPE,
    }
    start = time.time()
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(token_url, data=data)
        duration_ms = round((time.time() - start) * 1000, 1)
        logger.info(
            "hatif_token_response",
            extra={
                "extra": {
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                }
            },
        )
        response.raise_for_status()
        payload = response.json()
        return payload["access_token"], int(payload.get("expires_in", 3600))


async def get_access_token() -> str:
    return await _token_cache.get(_fetch_token)


async def send_whatsapp_template(
    template_name: str,
    to_number: str,
    parameters: list[str],
    language: str = "ar",
) -> tuple[bool, str, dict]:
    """
    Send a WhatsApp template via Hatif.

    *parameters*: body param values (positional, matching template {{1}}..{{N}}).
                  Empty values should be replaced with a placeholder BEFORE calling
                  this function — Hatif rejects empty body param values with 500.
    """
    token = await get_access_token()
    url = f"{settings.HATIF_BASE_URL.rstrip('/')}/v1/whatsapp/service-account/sendTemplate"

    # ── Build payload ──
    body: dict = {
        "ChannelId": settings.HATIF_CHANNEL_ID,
        "TemplateName": template_name,
        "Language": language,
        "ToNumber": to_number,
    }

    # Only include Parameters if there are body params (welcome has 0)
    if parameters:
        body["Parameters"] = [
            {
                "Type": "Body",
                "Values": [{"Type": "text", "Text": v} for v in parameters],
            }
        ]

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # ── Log outgoing payload (token masked) ──
    logger.info(
        "hatif_send_template_request",
        extra={
            "extra": {
                "url": url,
                "to": to_number,
                "template": template_name,
                "language": language,
                "param_count": len(parameters),
                "channel_id": settings.HATIF_CHANNEL_ID,
                "outgoing_payload": body,
            }
        },
    )

    start = time.time()
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(url, headers=headers, json=body)
        duration_ms = round((time.time() - start) * 1000, 1)
        success = 200 <= response.status_code < 300
        content = response.text
        try:
            response_json = _normalize_keys(response.json())
        except Exception:
            response_json = {}

    if success:
        logger.info(
            "hatif_send_template_success",
            extra={
                "extra": {
                    "to": to_number,
                    "template": template_name,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                    "conversation_event_id": response_json.get("conversationeventid"),
                    "contact_id": response_json.get("contactid"),
                }
            },
        )
    else:
        # ── Full diagnostic on failure ──
        logger.error(
            "hatif_send_template_failed",
            extra={
                "extra": {
                    "to": to_number,
                    "template": template_name,
                    "language": language,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                    "response_body": content[:1000],
                    "response_json": response_json,
                    "params_sent": parameters,
                    "outgoing_payload": body,
                }
            },
        )

    return success, content, response_json


async def send_whatsapp_text(
    to_number: str,
    text: str,
) -> tuple[bool, str, dict]:
    token = await get_access_token()
    url = f"{settings.HATIF_BASE_URL.rstrip('/')}/v1/whatsapp/service-account/sendText"
    body = {
        "ChannelId": settings.HATIF_CHANNEL_ID,
        "Text": text,
        "ToNumber": to_number,
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    logger.info(
        "hatif_send_text_request",
        extra={
            "extra": {
                "url": url,
                "to": to_number,
                "text_length": len(text),
                "channel_id": settings.HATIF_CHANNEL_ID,
            }
        },
    )

    start = time.time()
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(url, headers=headers, json=body)
        duration_ms = round((time.time() - start) * 1000, 1)
        success = 200 <= response.status_code < 300
        content = response.text
        try:
            response_json = _normalize_keys(response.json())
        except Exception:
            response_json = {}

    if success:
        logger.info(
            "hatif_send_text_success",
            extra={
                "extra": {
                    "to": to_number,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                    "conversation_event_id": response_json.get("conversationeventid"),
                    "contact_id": response_json.get("contactid"),
                }
            },
        )
    else:
        logger.error(
            "hatif_send_text_failed",
            extra={
                "extra": {
                    "to": to_number,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                    "response_body": content[:1000],
                    "response_json": response_json,
                }
            },
        )

    return success, content, response_json


def format_provider_response(success: bool, response_body: str) -> str:
    return json.dumps({"success": success, "response": response_body})
