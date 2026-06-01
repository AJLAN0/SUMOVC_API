import logging
import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import RoleRecipient
from app.services.rekaz import normalize_phone

logger = logging.getLogger("app.role_recipients")

NOTIFICATION_ROLES: dict[str, dict[str, str]] = {
    "admin": {
        "label_ar": "مشرف",
        "label_en": "Admin",
    },
    "product_technician": {
        "label_ar": "فني منتجات",
        "label_en": "Product Technician",
    },
    "portrait_technician": {
        "label_ar": "فني بورتريه",
        "label_en": "Portrait Technician",
    },
}

_ROLE_CACHE: dict[str, list[str]] | None = None
_ROLE_CACHE_AT: float = 0.0
_ROLE_CACHE_TTL = 30.0


def invalidate_role_cache() -> None:
    global _ROLE_CACHE, _ROLE_CACHE_AT
    _ROLE_CACHE = None
    _ROLE_CACHE_AT = 0.0


def is_valid_role(role: str) -> bool:
    return role in NOTIFICATION_ROLES


def get_phones_for_role(db: Session, role: str) -> list[str]:
    if not is_valid_role(role):
        return []

    global _ROLE_CACHE, _ROLE_CACHE_AT
    now = time.time()
    if _ROLE_CACHE is None or (now - _ROLE_CACHE_AT) >= _ROLE_CACHE_TTL:
        rows = db.execute(
            select(RoleRecipient).where(RoleRecipient.enabled.is_(True))
        ).scalars().all()
        cache: dict[str, list[str]] = {key: [] for key in NOTIFICATION_ROLES}
        for row in rows:
            if row.role in cache:
                cache[row.role].append(row.phone)
        _ROLE_CACHE = cache
        _ROLE_CACHE_AT = now

    return list(_ROLE_CACHE.get(role, []))


def list_recipients_by_role(db: Session) -> dict[str, list[dict[str, Any]]]:
    rows = db.execute(
        select(RoleRecipient).order_by(RoleRecipient.role, RoleRecipient.created_at)
    ).scalars().all()
    grouped: dict[str, list[dict[str, Any]]] = {role: [] for role in NOTIFICATION_ROLES}
    for row in rows:
        if row.role not in grouped:
            continue
        grouped[row.role].append(
            {
                "id": row.id,
                "phone": row.phone,
                "label": row.label,
                "enabled": row.enabled,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
        )
    return grouped


def add_recipient(
    db: Session,
    role: str,
    phone_raw: str,
    label: str | None = None,
    enabled: bool = True,
) -> RoleRecipient:
    if not is_valid_role(role):
        raise ValueError(f"invalid_role:{role}")
    phone = normalize_phone(phone_raw.strip())
    if not phone:
        raise ValueError("invalid_phone")

    row = RoleRecipient(role=role, phone=phone, label=(label or "").strip() or None, enabled=enabled)
    db.add(row)
    db.commit()
    db.refresh(row)
    invalidate_role_cache()
    return row


def seed_role_recipients(db: Session) -> None:
    """Seed admin role from ADMIN_TO_NUMBERS if no recipients exist yet."""
    existing = db.execute(select(RoleRecipient.id).limit(1)).scalar_one_or_none()
    if existing:
        return

    admin_phones = settings.admin_numbers()
    for phone in admin_phones:
        db.add(RoleRecipient(role="admin", phone=phone, enabled=True))
    if admin_phones:
        db.commit()
        logger.info(
            "role_recipients_seeded_from_env",
            extra={"extra": {"role": "admin", "count": len(admin_phones)}},
        )
