"""WhatsApp template definitions stored in DB with in-memory cache."""

import json
import logging
import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

logger = logging.getLogger("app.template_catalog")

# Fallback when DB empty (matches original code)
DEFAULT_TEMPLATE_SPECS: dict[str, list[str]] = {
    "reservation_confirmedddddddd": [
        "customer_name",
        "product_name",
        "reservation_date",
        "start_time",
        "end_time",
        "branch_name",
    ],
    "reservation_reminderrrr": ["customer_name", "branch_name"],
    "reservation_cancelled": ["customer_name", "cancel_reason"],
    "admin_reservation_confirmedddd": [
        "customer_name",
        "reservation_date",
        "start_time",
        "product_name",
        "branch_name",
    ],
    "gift_clint_send": [
        "recipient_name",
        "from_name",
        "message",
        "product_name",
        "redemption_url",
    ],
    "sent_gifft": [
        "recipient_name",
        "from_name",
        "message",
        "product_name",
        "redemption_url",
    ],
    "gifft_send": [
        "recipient_name",
        "from_name",
        "message",
        "product_name",
        "redemption_url",
    ],
    "product_clint_done": ["product_name"],
    "product_done_clint": ["product_name"],
    "product_done_admin": ["customer_name", "product_name"],
}

DEFAULT_TEMPLATE_META: dict[str, dict[str, str]] = {
    "reservation_confirmedddddddd": {"title_ar": "تأكيد الحجز للعميل", "description": "رسالة تأكيد الحجز"},
    "reservation_reminderrrr": {"title_ar": "تذكير بالموعد", "description": "تذكير قبل الموعد"},
    "reservation_cancelled": {"title_ar": "إلغاء الحجز", "description": "رسالة إلغاء"},
    "admin_reservation_confirmedddd": {
        "title_ar": "تأكيد للمشرف",
        "description": "إشعار المشرفين",
    },
    "gift_clint_send": {
        "title_ar": "إرسال الهدية للمستلم",
        "description": "رسالة واتساب للمستلم (RecipientCustomer) — 5 متغيرات",
    },
    "sent_gifft": {
        "title_ar": "إرسال الهدية للمستلم (قديم)",
        "description": "استخدم gift_clint_send",
    },
    "gifft_send": {
        "title_ar": "إرسال الهدية للمستلم (قديم)",
        "description": "استخدم gift_clint_send",
    },
    "product_clint_done": {
        "title_ar": "تأكيد شراء المنتج للعميل",
        "description": "MerchandiseOrderCompletedEvent — اسم المنتج {{1}} + زر استلمت الشراء",
    },
    "product_done_clint": {
        "title_ar": "تأكيد شراء المنتج للعميل (قديم)",
        "description": "استخدم product_clint_done",
    },
    "product_done_admin": {
        "title_ar": "إشعار طلب منتجات للفريق",
        "description": "MerchandiseOrderCompletedEvent — فني منتجات — اسم العميل {{1}} + المنتج {{2}}",
    },
}

# Arabic labels for Rekaz field keys (form hints)
PARAM_LABELS_AR: dict[str, str] = {
    "customer_name": "اسم العميل",
    "product_name": "اسم الباقة / المنتج",
    "reservation_date": "تاريخ الحجز",
    "start_time": "وقت البداية",
    "end_time": "وقت النهاية",
    "branch_name": "اسم الفرع",
    "cancel_reason": "سبب الإلغاء",
    "recipient_name": "اسم المستلم",
    "buyer_name": "اسم المشتري",
    "to_name": "إلى (المستلم)",
    "from_name": "من (المرسل)",
    "message": "رسالة الهدية",
    "redemption_url": "رابط الاسترداد",
    "gift_coupon_code": "كود الهدية",
    "gift_theme_name": "ثيم الهدية",
    "total_price": "السعر الإجمالي",
    "price_name": "فئة السعر",
    "order_code": "رقم الطلب",
    "gift_id": "معرّف الهدية",
    "entity_id": "معرّف الكيان",
    "subscription_number": "رقم الاشتراك",
    "subscription_code": "كود الاشتراك",
    "items_summary": "ملخص المنتجات",
    "discount": "الخصم",
    "status": "الحالة",
    "end_date": "تاريخ النهاية",
    "payload_kind": "نوع الحمولة",
}

_SPECS_CACHE: dict[str, list[str]] | None = None
_META_CACHE: dict[str, dict[str, Any]] | None = None
_CACHE_AT: float = 0.0
_CACHE_TTL = 30.0


def invalidate_template_cache() -> None:
    global _SPECS_CACHE, _META_CACHE, _CACHE_AT
    _SPECS_CACHE = None
    _META_CACHE = None
    _CACHE_AT = 0.0


def _parse_param_keys(raw: str) -> list[str]:
    """One key per line or comma-separated."""
    if not raw or not raw.strip():
        return []
    keys: list[str] = []
    for line in raw.replace(",", "\n").split("\n"):
        k = line.strip()
        if k and k not in keys:
            keys.append(k)
    return keys


def param_keys_to_json(keys: list[str]) -> str:
    return json.dumps(keys, ensure_ascii=False)


def param_keys_from_json(raw: str) -> list[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    return _parse_param_keys(raw)


def label_for_param(key: str) -> str:
    return PARAM_LABELS_AR.get(key, key.replace("_", " "))


def load_template_catalog(db: Session) -> tuple[dict[str, list[str]], dict[str, dict[str, Any]]]:
    global _SPECS_CACHE, _META_CACHE, _CACHE_AT
    now = time.time()
    if _SPECS_CACHE is not None and (now - _CACHE_AT) < _CACHE_TTL:
        return _SPECS_CACHE, _META_CACHE or {}

    from app.models import WhatsAppTemplate

    rows = db.execute(
        select(WhatsAppTemplate).where(WhatsAppTemplate.enabled.is_(True)).order_by(WhatsAppTemplate.name)
    ).scalars().all()

    specs: dict[str, list[str]] = {}
    meta: dict[str, dict[str, Any]] = {}
    for row in rows:
        keys = param_keys_from_json(row.param_keys_json)
        if keys:
            specs[row.name] = keys
            meta[row.name] = {
                "id": row.id,
                "title_ar": row.title_ar or row.name,
                "description": row.description or "",
            }

    if not specs:
        specs = dict(DEFAULT_TEMPLATE_SPECS)
        meta = {k: {**v, "id": None} for k, v in DEFAULT_TEMPLATE_META.items()}

    _SPECS_CACHE = specs
    _META_CACHE = meta
    _CACHE_AT = now
    return specs, meta


def get_template_specs(db: Session | None = None) -> dict[str, list[str]]:
    if db is not None:
        specs, _ = load_template_catalog(db)
        return specs
    if _SPECS_CACHE is not None:
        return _SPECS_CACHE
    return DEFAULT_TEMPLATE_SPECS


def get_spec_for_template(db: Session, template_name: str) -> list[str]:
    specs, _ = load_template_catalog(db)
    return specs.get(template_name, DEFAULT_TEMPLATE_SPECS.get(template_name, []))


def _template_row_dict(r) -> dict[str, Any]:
    keys = param_keys_from_json(r.param_keys_json)
    return {
        "id": r.id,
        "name": r.name,
        "title_ar": r.title_ar or r.name,
        "description": r.description or "",
        "param_keys": keys,
        "param_keys_text": "\n".join(keys),
        "param_count": len(keys),
        "enabled": r.enabled,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def _default_template_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": None,
            "name": name,
            "title_ar": DEFAULT_TEMPLATE_META.get(name, {}).get("title_ar", name),
            "description": DEFAULT_TEMPLATE_META.get(name, {}).get("description", ""),
            "param_keys": keys,
            "param_keys_text": "\n".join(keys),
            "param_count": len(keys),
            "enabled": True,
            "updated_at": None,
        }
        for name, keys in DEFAULT_TEMPLATE_SPECS.items()
    ]


def list_all_templates(db: Session) -> list[dict[str, Any]]:
    from app.models import WhatsAppTemplate

    rows = db.execute(select(WhatsAppTemplate).order_by(WhatsAppTemplate.name)).scalars().all()
    if not rows:
        return _default_template_rows()
    return [_template_row_dict(r) for r in rows]


def list_enabled_templates(db: Session) -> list[dict[str, Any]]:
    return [t for t in list_all_templates(db) if t.get("enabled")]


def seed_whatsapp_templates(db: Session) -> None:
    from app.models import WhatsAppTemplate

    for name, keys in DEFAULT_TEMPLATE_SPECS.items():
        existing = db.execute(
            select(WhatsAppTemplate).where(WhatsAppTemplate.name == name)
        ).scalar_one_or_none()
        if existing:
            continue
        meta = DEFAULT_TEMPLATE_META.get(name, {})
        db.add(
            WhatsAppTemplate(
                name=name,
                title_ar=meta.get("title_ar", name),
                description=meta.get("description"),
                param_keys_json=param_keys_to_json(keys),
                enabled=True,
            )
        )
    db.commit()
    invalidate_template_cache()


def build_params_from_form(spec: list[str], form_data: dict, placeholder: str = "-") -> list[str]:
    """Build ordered param list from form keys param__{key} or param_{key}."""
    out: list[str] = []
    for key in spec:
        val = form_data.get(f"param__{key}") or form_data.get(f"param_{key}") or ""
        out.append((str(val).strip() if val else "") or placeholder)
    return out
