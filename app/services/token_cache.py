import asyncio
import logging
import time

logger = logging.getLogger("app.token_cache")


class TokenCache:
    def __init__(self) -> None:
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._lock = asyncio.Lock()

    async def get(self, fetcher) -> str:
        now = time.time()
        if self._token and now < self._expires_at:
            ttl = round(self._expires_at - now, 1)
            logger.debug(
                "token_cache_hit",
                extra={"extra": {"ttl_seconds": ttl}},
            )
            return self._token

        logger.info("token_cache_miss_acquiring_lock")
        async with self._lock:
            # Double-check after acquiring lock
            now = time.time()
            if self._token and now < self._expires_at:
                ttl = round(self._expires_at - now, 1)
                logger.debug(
                    "token_cache_hit_after_lock",
                    extra={"extra": {"ttl_seconds": ttl}},
                )
                return self._token

            logger.info("token_cache_refreshing")
            start = time.time()
            try:
                token, expires_in = await fetcher()
                self._token = token
                self._expires_at = time.time() + max(expires_in - 30, 30)
                duration_ms = round((time.time() - start) * 1000, 1)
                logger.info(
                    "token_cache_refreshed",
                    extra={
                        "extra": {
                            "expires_in": expires_in,
                            "effective_ttl": max(expires_in - 30, 30),
                            "duration_ms": duration_ms,
                        }
                    },
                )
                return self._token
            except Exception:
                duration_ms = round((time.time() - start) * 1000, 1)
                logger.error(
                    "token_cache_refresh_failed",
                    extra={"extra": {"duration_ms": duration_ms}},
                    exc_info=True,
                )
                raise
