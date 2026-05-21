import asyncio
import logging
import time
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from app.admin.errors import format_api_error
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

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


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if request.url.path.startswith("/admin/api"):
        return JSONResponse(
            status_code=exc.status_code,
            content=format_api_error(exc.status_code, exc.detail),
        )
    detail = exc.detail
    if isinstance(detail, dict):
        return JSONResponse(status_code=exc.status_code, content=detail)
    return JSONResponse(status_code=exc.status_code, content={"detail": detail})

app.add_middleware(
    SessionMiddleware,
    secret_key=settings.effective_session_secret(),
    session_cookie="sumo_admin",
    max_age=60 * 60 * 24 * 7,
    same_site="lax",
    https_only=settings.ADMIN_COOKIE_SECURE,
)

_static_dir = __import__("pathlib").Path(__file__).parent / "admin" / "static"
app.mount("/admin/static", StaticFiles(directory=str(_static_dir)), name="admin-static")

from app.admin.api import router as admin_api_router  # noqa: E402
from app.admin.router import router as admin_pages_router  # noqa: E402

app.include_router(admin_pages_router)
app.include_router(admin_api_router)
app.include_router(rekaz_webhook.router)
app.include_router(hatif_webhook.router)

logger.info(
    "app_started",
    extra={
        "extra": {
            "send_mode": settings.HATIF_SEND_MODE,
            "database": settings.DATABASE_URL,
            "hatif_base_url": settings.HATIF_BASE_URL,
            "admin_numbers": settings.admin_numbers(),
            "reminder_before_minutes": settings.REMINDER_BEFORE_MINUTES,
            "allowed_late_minutes": settings.ALLOWED_LATE_MINUTES,
        }
    },
)


# ── Startup: launch reminder worker ────────────────────────────────────

@app.on_event("startup")
async def _startup():
    from app.services.reminder_worker import reminder_worker_loop  # noqa: E402

    asyncio.create_task(reminder_worker_loop())
    logger.info("reminder_worker_task_created")


# ── Middleware ──────────────────────────────────────────────────────────

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


# ── Health endpoint ─────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
async def health():
    return {"status": "ok"}
