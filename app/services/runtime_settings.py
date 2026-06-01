import logging
import time

from sqlalchemy.orm import Session

from app.config import settings
from app.models import AppSetting

logger = logging.getLogger("app.runtime_settings")

SETTING_REMINDER_BEFORE_MINUTES = "reminder_before_minutes"
SETTING_ALLOWED_LATE_MINUTES = "allowed_late_minutes"

_CACHE: dict[str, str] | None = None
_CACHE_AT: float = 0.0
_CACHE_TTL = 30.0


def invalidate_settings_cache() -> None:
    global _CACHE, _CACHE_AT
    _CACHE = None
    _CACHE_AT = 0.0


def _load_cache(db: Session) -> dict[str, str]:
    global _CACHE, _CACHE_AT
    now = time.time()
    if _CACHE is not None and (now - _CACHE_AT) < _CACHE_TTL:
        return _CACHE

    rows = db.query(AppSetting).all()
    _CACHE = {row.key: row.value for row in rows}
    _CACHE_AT = now
    return _CACHE


def get_setting(db: Session, key: str) -> str | None:
    return _load_cache(db).get(key)


def set_setting(db: Session, key: str, value: str) -> None:
    row = db.get(AppSetting, key)
    if row:
        row.value = value
        from datetime import datetime

        row.updated_at = datetime.utcnow()
    else:
        row = AppSetting(key=key, value=value)
        db.add(row)
    db.commit()
    invalidate_settings_cache()
    logger.info("app_setting_updated", extra={"extra": {"key": key}})


def get_reminder_before_minutes(db: Session) -> int:
    raw = get_setting(db, SETTING_REMINDER_BEFORE_MINUTES)
    if raw is not None:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return settings.REMINDER_BEFORE_MINUTES


def get_allowed_late_minutes(db: Session) -> int:
    raw = get_setting(db, SETTING_ALLOWED_LATE_MINUTES)
    if raw is not None:
        try:
            return max(0, int(raw))
        except ValueError:
            pass
    return settings.ALLOWED_LATE_MINUTES


def get_runtime_settings_view(db: Session) -> dict[str, dict]:
    """Dashboard view: effective values + whether overridden in DB."""
    cache = _load_cache(db)
    reminder_raw = cache.get(SETTING_REMINDER_BEFORE_MINUTES)
    allowed_raw = cache.get(SETTING_ALLOWED_LATE_MINUTES)
    return {
        "reminder_before_minutes": {
            "value": get_reminder_before_minutes(db),
            "env_default": settings.REMINDER_BEFORE_MINUTES,
            "overridden": reminder_raw is not None,
        },
        "allowed_late_minutes": {
            "value": get_allowed_late_minutes(db),
            "env_default": settings.ALLOWED_LATE_MINUTES,
            "overridden": allowed_raw is not None,
        },
    }


def seed_app_settings(db: Session) -> None:
    """No-op if rows exist; env remains fallback until dashboard saves."""
    existing = db.query(AppSetting).count()
    if existing:
        return
