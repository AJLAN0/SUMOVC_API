import hashlib
import hmac
import logging

logger = logging.getLogger("app.signature")


def compute_hmac_sha256_hex(body_utf8: str, secret: str) -> str:
    return hmac.new(secret.encode(), body_utf8.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_voxa_signature(body_utf8: str, secret: str, signature: str | None) -> bool:
    if not signature or not secret:
        logger.debug(
            "signature_verify_skip",
            extra={"extra": {"has_signature": bool(signature), "has_secret": bool(secret)}},
        )
        return False

    digest = compute_hmac_sha256_hex(body_utf8, secret).lower()
    received = signature.strip().lower()
    match = hmac.compare_digest(digest, received)

    logger.debug(
        "signature_verify_result",
        extra={
            "extra": {
                "match": match,
                "computed_prefix": digest[:8] + "...",
                "received_prefix": received[:8] + "..." if len(received) > 8 else received,
                "body_length": len(body_utf8),
            }
        },
    )

    if not match:
        logger.warning(
            "signature_mismatch",
            extra={
                "extra": {
                    "computed": digest[:16] + "...",
                    "received": received[:16] + "..." if len(received) > 16 else received,
                }
            },
        )

    return match
