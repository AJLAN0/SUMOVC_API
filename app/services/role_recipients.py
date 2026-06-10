import logging
import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import RoleRecipient
from app.services.rekaz import normalize_phone

logger = logging.getLogger("app.role_recipients")

STAFF_NOTIFICATION_ROLES: dict[str, dict[str, str]] = {
    "portrait_technician": {
        "label_ar": "فني بورتريه",
        "label_en": "Portrait Technician",
    },
    "product_technician": {
        "label_ar": "فني منتجات",
        "label_en": "Product Technician",
    },
}

# Backward-compatible alias (staff alerts use STAFF_NOTIFICATION_ROLES only).
NOTIFICATION_ROLES = STAFF_NOTIFICATION_ROLES

_LEGACY_ROLE_LABELS: dict[str, dict[str, str]] = {
    "admin": {"label_ar": "مشرف (قديم)", "label_en": "Admin (legacy)"},
}

_MERCHANDISE_EVENT_PREFIX = "Merchandise"

DEFAULT_PORTRAIT_TECHNICIAN_PHONES = (
    "966550556381",
    "966583771046",
    "966554818612",
)

DEFAULT_PRODUCT_TECHNICIAN_PHONES = (
    "966550556381",
    "966547537826",
    "966557019152",
)

_ROLE_CACHE: dict[str, list[str]] | None = None
_ROLE_CACHE_AT: float = 0.0
_ROLE_CACHE_TTL = 30.0


def invalidate_role_cache() -> None:
    global _ROLE_CACHE, _ROLE_CACHE_AT
    _ROLE_CACHE = None
    _ROLE_CACHE_AT = 0.0


def is_valid_role(role: str) -> bool:
    return role in STAFF_NOTIFICATION_ROLES


def role_display_meta(role: str | None) -> dict[str, str]:
    if not role:
        return {"label_ar": "—", "label_en": "—"}
    if role in STAFF_NOTIFICATION_ROLES:
        return STAFF_NOTIFICATION_ROLES[role]
    return _LEGACY_ROLE_LABELS.get(role, {"label_ar": role, "label_en": role})


def resolve_staff_role(role: str | None, event_name: str | None = None) -> str | None:
    """Map DB / legacy values to portrait_technician or product_technician."""
    cleaned = (role or "").strip()
    if cleaned in STAFF_NOTIFICATION_ROLES:
        return cleaned
    if event_name and event_name.startswith(_MERCHANDISE_EVENT_PREFIX):
        return "product_technician"
    if cleaned == "admin" or not cleaned:
        return "portrait_technician"
    return None


def get_phones_for_role(db: Session, role: str) -> list[str]:
    if not is_valid_role(role):
        return []

    global _ROLE_CACHE, _ROLE_CACHE_AT
    now = time.time()
    if _ROLE_CACHE is None or (now - _ROLE_CACHE_AT) >= _ROLE_CACHE_TTL:
        rows = db.execute(
            select(RoleRecipient).where(RoleRecipient.enabled.is_(True))
        ).scalars().all()
        cache: dict[str, list[str]] = {key: [] for key in STAFF_NOTIFICATION_ROLES}
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
    grouped: dict[str, list[dict[str, Any]]] = {role: [] for role in STAFF_NOTIFICATION_ROLES}
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
    """Seed default phones for portrait and product technician roles."""
    _ensure_role_phones(db, "portrait_technician", DEFAULT_PORTRAIT_TECHNICIAN_PHONES)
    _ensure_role_phones(db, "product_technician", DEFAULT_PRODUCT_TECHNICIAN_PHONES)

    for role in STAFF_NOTIFICATION_ROLES:
        count = len(get_phones_for_role(db, role))
        logger.info(
            "role_recipients_ready",
            extra={"extra": {"role": role, "enabled_phone_count": count}},
        )


def _ensure_role_phones(db: Session, role: str, raw_phones: tuple[str, ...]) -> None:
    """Add default phones for a role if they are not already configured."""
    if not is_valid_role(role):
        return
    rows = db.execute(select(RoleRecipient).where(RoleRecipient.role == role)).scalars().all()
    existing = {row.phone for row in rows}
    added = 0
    for raw in raw_phones:
        phone = normalize_phone(raw.strip())
        if not phone or phone in existing:
            continue
        db.add(RoleRecipient(role=role, phone=phone, enabled=True))
        existing.add(phone)
        added += 1
    if added:
        db.commit()
        invalidate_role_cache()
        logger.info(
            "role_recipients_seeded_defaults",
            extra={"extra": {"role": role, "count": added}},
        )
