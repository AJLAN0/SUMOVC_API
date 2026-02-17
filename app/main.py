import logging
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.config import settings
from app.logging_config import configure_logging

configure_logging()

logger = logging.getLogger("app")

# Log loaded config (secrets masked)
settings.log_summary()

from app.database import init_db  # noqa: E402
from app.routers import hatif_webhook, rekaz_webhook  # noqa: E402
from app.schemas import HealthResponse  # noqa: E402

init_db()

app = FastAPI(title="Rekaz-Hatif Middleware")

app.include_router(rekaz_webhook.router)
app.include_router(hatif_webhook.router)

logger.info(
    "app_started",
    extra={
        "extra": {
            "send_mode": settings.HATIF_SEND_MODE,
            "database": settings.DATABASE_URL,
            "hatif_base_url": settings.HATIF_BASE_URL,
        }
    },
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    start = time.time()

    method = request.method
    path = request.url.path

    logger.info(
        "request_started",
        extra={
            "extra": {
                "request_id": request_id,
                "method": method,
                "path": path,
                "client": request.client.host if request.client else "unknown",
            }
        },
    )

    try:
        response = await call_next(request)
        duration_ms = round((time.time() - start) * 1000, 1)
        response.headers["X-Request-Id"] = request_id

        logger.info(
            "request_completed",
            extra={
                "extra": {
                    "request_id": request_id,
                    "method": method,
                    "path": path,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                }
            },
        )
        return response
    except Exception:
        duration_ms = round((time.time() - start) * 1000, 1)
        logger.error(
            "request_failed",
            extra={
                "extra": {
                    "request_id": request_id,
                    "method": method,
                    "path": path,
                    "duration_ms": duration_ms,
                }
            },
            exc_info=True,
        )
        return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.get("/health", response_model=HealthResponse)
async def health():
    return {"status": "ok"}
