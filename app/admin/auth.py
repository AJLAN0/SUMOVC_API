import time
from collections import defaultdict

from fastapi import HTTPException, Request, status
from fastapi.responses import RedirectResponse

from app.config import settings

SESSION_KEY = "admin_email"
_LOGIN_WINDOW_SEC = 900
_MAX_FAILURES = 5
_failures: dict[str, list[float]] = defaultdict(list)


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _prune_failures(ip: str) -> None:
    now = time.time()
    _failures[ip] = [t for t in _failures[ip] if now - t < _LOGIN_WINDOW_SEC]


def record_login_failure(request: Request) -> None:
    ip = _client_ip(request)
    _prune_failures(ip)
    _failures[ip].append(time.time())


def is_login_rate_limited(request: Request) -> bool:
    ip = _client_ip(request)
    _prune_failures(ip)
    return len(_failures[ip]) >= _MAX_FAILURES


def clear_login_failures(request: Request) -> None:
    ip = _client_ip(request)
    _failures.pop(ip, None)


def authenticate(email: str, password: str) -> bool:
    if not settings.admin_configured():
        return False
    if email.strip().lower() != settings.ADMIN_EMAIL:
        return False
    return settings.verify_admin_password(password)


def login_session(request: Request, email: str) -> None:
    request.session[SESSION_KEY] = email.strip().lower()


def logout_session(request: Request) -> None:
    request.session.pop(SESSION_KEY, None)


def is_authenticated(request: Request) -> bool:
    if not settings.admin_configured():
        return False
    return request.session.get(SESSION_KEY) == settings.ADMIN_EMAIL


def _admin_not_configured() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Admin dashboard is not configured. Set ADMIN_EMAIL, ADMIN_PASSWORD_HASH, ADMIN_SESSION_SECRET.",
    )


def require_admin_api(request: Request) -> str:
    if not settings.admin_configured():
        raise _admin_not_configured()
    if not is_authenticated(request):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return settings.ADMIN_EMAIL


def require_admin_page(request: Request) -> str | RedirectResponse:
    if not settings.admin_configured():
        raise _admin_not_configured()
    if not is_authenticated(request):
        return RedirectResponse(url="/login", status_code=302)
    return settings.ADMIN_EMAIL
