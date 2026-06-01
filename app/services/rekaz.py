import logging
import re
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

logger = logging.getLogger("app.rekaz")

# Rekaz reservation times are Saudi local; API often tags them as +00:00 or Z (not true UTC).
_RIYADH = timezone(timedelta(hours=3))

# ── Event → Template mapping ───────────────────────────────────────────
EVENT_TEMPLATE_MAP = {
    "ReservationConfirmedEvent": "reservation_confirmedddddddd",
    "ReservationCancelledEvent": "reservation_cancelled",
    "GiftCreatedEvent": "gifft_send",
}

GIFT_EVENT_NAMES = frozenset({"GiftCreatedEvent"})
MERCHANDISE_EVENT_NAMES = frozenset({"MerchandiseOrderCreatedEvent"})

# ── Template → ordered BODY parameter names ────────────────────────────
#
#  ⚠️  These MUST match the exact variable count & order in Hatif UI.
#       Sending the wrong number of body params → 500 from Hatif.
#       Empty body params ("") also cause 500 — use EMPTY_PARAM_PLACEHOLDER.
#
TEMPLATE_PARAM_SPECS: dict[str, list[str]] = {
    # client – confirmed  (6 body vars, TEXT title — no header image)
    "reservation_confirmedddddddd": [
        "customer_name",      # {{1}}
        "product_name",       # {{2}}
        "reservation_date",   # {{3}}
        "start_time",         # {{4}}
        "end_time",           # {{5}}
        "branch_name",        # {{6}}
    ],

    # client – reminder  (2 body vars)
    "reservation_reminderrrr": [
        "customer_name",      # {{1}}
        "branch_name",        # {{2}}
    ],

    # client – cancelled  (2 body vars)
    "reservation_cancelled": [
        "customer_name",
        "cancel_reason",
    ],

    # admin  (5 body vars — matches Hatif UI)
    "admin_reservation_confirmedddd": [
        "customer_name",      # {{1}} GUEST NAME
        "reservation_date",   # {{2}} DATE
        "start_time",         # {{3}} TIME
        "product_name",       # {{4}} PACKAGE NAME
        "branch_name",        # {{5}} LOCATION
    ],

    # gift – sent to RecipientCustomer (2 body vars — Hatif template gifft_send)
    "gifft_send": [
        "from_name",          # {{1}} giver name
        "gift_coupon_code",   # {{2}} coupon or redemption token
    ],
    # legacy alias
    "sent_gifft": [
        "from_name",
        "gift_coupon_code",
    ],

}

# ── Default fallback spec (if template not in TEMPLATE_PARAM_SPECS) ────
_FALLBACK_SPEC = ["customer_name", "product_name", "reservation_date", "start_time"]


# ── Helpers ─────────────────────────────────────────────────────────────
def map_event_to_template(db: Session, event_name: str | None) -> str | None:
    if not event_name:
        logger.debug("map_event_to_template called with empty event_name")
        return None
    from app.admin.services import resolve_template_for_event

    template = resolve_template_for_event(db, event_name)
    logger.info(
        "event_template_mapped",
        extra={"extra": {"event_name": event_name, "template": template, "mapped": template is not None}},
    )
    return template


def normalize_phone(phone: str | None) -> str | None:
    if not phone:
        logger.debug("normalize_phone called with empty phone")
        return None
    digits = re.sub(r"\D", "", phone)
    original = digits
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("0") and len(digits) == 10:
        digits = "966" + digits[1:]
    elif digits.startswith("5") and len(digits) == 9:
        digits = "966" + digits
    logger.debug(
        "phone_normalized",
        extra={"extra": {"input": phone, "digits_extracted": original, "normalized": digits}},
    )
    return digits


# ── Date / time parsing & formatting ───────────────────────────────────

def _parse_dt(value: str | None) -> datetime | None:
    """Parse an ISO-ish datetime string (with or without trailing Z)."""
    if not value:
        return None
    try:
        v = value
        if v.endswith("Z"):
            v = v.replace("Z", "+00:00")
        return datetime.fromisoformat(v)
    except Exception:
        return None


def rekaz_start_to_utc(start_raw: str | None) -> datetime | None:
    """
    Convert Rekaz ``startDate`` to naive UTC for reminder scheduling.

    Rekaz typically sends Asia/Riyadh wall time in one of these forms:
    - ``2026-05-15T16:45:00`` (naive)
    - ``2026-05-15T16:45:00+00:00`` or ``...Z`` (local time mislabeled as UTC)

    True ``+03:00`` offsets are converted normally.
    """
    if not start_raw or not str(start_raw).strip():
        return None

    dt = _parse_dt(str(start_raw).strip())
    if not dt:
        return None

    if dt.tzinfo is None:
        local = dt.replace(tzinfo=_RIYADH)
        return local.astimezone(timezone.utc).replace(tzinfo=None)

    offset = dt.utcoffset()
    if offset is not None and offset.total_seconds() == 0:
        # Mislabeled UTC: keep clock time, interpret as Riyadh
        wall = dt.replace(tzinfo=None)
        local = wall.replace(tzinfo=_RIYADH)
        return local.astimezone(timezone.utc).replace(tzinfo=None)

    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _fmt_date(dt: datetime | None) -> str:
    """YYYY-MM-DD"""
    return dt.strftime("%Y-%m-%d") if dt else ""


def _fmt_time(dt: datetime | None) -> str:
    """HH:MM (24-hour)"""
    return dt.strftime("%H:%M") if dt else ""


def _fmt_iso(value: str | None) -> str:
    """Return a cleaned ISO string (Z → +00:00) for internal scheduling use."""
    if not value:
        return ""
    v = value
    if v.endswith("Z"):
        v = v.replace("Z", "+00:00")
    return v


# ── Case-insensitive key lookup ────────────────────────────────────────

def _ci(d: dict, *keys: str):
    """Try each key as-is, then lower, then Title-cased."""
    if not d:
        return None
    for key in keys:
        for variant in (key, key.lower(), key[0].upper() + key[1:] if key else key):
            if variant in d:
                return d[variant]
    return None


def is_gift_event(event_name: str | None) -> bool:
    return bool(event_name and event_name in GIFT_EVENT_NAMES)


def is_merchandise_event(event_name: str | None) -> bool:
    return bool(event_name and event_name in MERCHANDISE_EVENT_NAMES)


def _gift_payload(data: dict) -> bool:
    """Detect gift-shaped payloads even if EventName is missing."""
    return bool(_ci(data, "RecipientCustomer", "recipientCustomer") or _ci(data, "RedemptionUrl", "redemptionUrl"))


def resolve_message_phone(payload: dict, event_name: str | None) -> tuple[str | None, str]:
    """
    Resolve the WhatsApp recipient phone from a Rekaz payload.

    Gifts → RecipientCustomer (the person who receives the gift card).
    Merchandise / reservations → Customer.
    """
    data = payload.get("Data") or payload.get("data") or {}

    if is_gift_event(event_name) or _gift_payload(data):
        recipient = data.get("RecipientCustomer") or data.get("recipientCustomer") or {}
        phone_raw = _ci(recipient, "MobileNumber", "mobileNumber", "phone")
        return phone_raw, "recipient_customer"

    if is_merchandise_event(event_name):
        customer = data.get("Customer") or data.get("customer") or {}
        phone_raw = _ci(customer, "MobileNumber", "mobileNumber", "phone")
        return phone_raw, "customer"

    customer = data.get("customer") or data.get("Customer") or {}
    phone_raw = _ci(customer, "MobileNumber", "mobileNumber", "phone")
    return phone_raw, "customer"


def _gift_redemption_code(data: dict) -> str:
    """Coupon code from payload, or last segment of RedemptionUrl for giftable products."""
    code = _ci(data, "giftCouponCode", "GiftCouponCode")
    if code:
        return str(code).strip()
    url = _ci(data, "redemptionUrl", "RedemptionUrl") or ""
    if url:
        token = str(url).rstrip("/").split("/")[-1]
        if token:
            return token
    return ""


def _gift_from_name(data: dict, buyer: dict) -> str:
    """Giver display name for gift template {{1}}."""
    from_name = _ci(data, "fromName", "FromName")
    if from_name:
        return str(from_name).strip()
    buyer_name = _ci(buyer, "name", "Name") or ""
    show_buyer = _ci(data, "showBuyerInfo", "ShowBuyerInfo")
    if show_buyer in (True, "true", "True", 1, "1"):
        return str(buyer_name).strip()
    # Admin / hidden buyer gifts still need a non-empty Hatif param
    return str(buyer_name).strip() if buyer_name else "-"


def resolve_correlation_id(fields: dict[str, str], event_name: str | None) -> str | None:
    """Idempotency key source: gift id, order code, or reservation number."""
    if is_gift_event(event_name):
        return fields.get("gift_id") or fields.get("reservation_number") or None
    if is_merchandise_event(event_name):
        return fields.get("order_code") or fields.get("reservation_number") or None
    return fields.get("reservation_number") or None


def resolve_template_language(payload: dict, event_name: str | None, default: str) -> str:
    data = payload.get("Data") or payload.get("data") or {}
    lang = _ci(data, "Language", "language")
    if lang and str(lang).strip():
        return str(lang).strip().lower()[:2]
    return default


# ── Extract all known fields from Rekaz payload ────────────────────────

def extract_fields(payload: dict, event_name: str | None = None) -> dict[str, str]:
    """
    Pull every field the templates might need out of a Rekaz webhook payload
    and return a flat ``{param_name: value}`` dict.

    Dates are parsed and formatted so templates receive clean YYYY-MM-DD / HH:MM.
    Raw ISO datetime strings are also included as ``start_dt_iso`` / ``end_dt_iso``
    for internal use (e.g. scheduling reminders).
    """
    event_name = event_name or payload.get("EventName") or payload.get("eventName") or ""
    data = payload.get("Data") or payload.get("data") or {}

    buyer = data.get("BuyerCustomer") or data.get("buyerCustomer") or {}
    recipient = data.get("RecipientCustomer") or data.get("recipientCustomer") or {}
    customer = data.get("customer") or data.get("Customer") or {}

    if is_gift_event(event_name) or _gift_payload(data):
        customer = recipient
    elif is_merchandise_event(event_name):
        customer = data.get("Customer") or data.get("customer") or customer

    # ── Raw datetime strings ──
    start_raw = _ci(
        data,
        "startDate",
        "StartDate",
        "reservationDate",
        "ReservationDate",
        "creationTime",
        "CreationTime",
    )
    end_raw = _ci(data, "endDate", "EndDate")

    start_dt = _parse_dt(start_raw)
    end_dt = _parse_dt(end_raw)

    recipient_name = _ci(recipient, "name", "Name") or _ci(data, "toName", "ToName") or ""
    buyer_name = _ci(buyer, "name", "Name") or _ci(data, "fromName", "FromName") or ""
    gift_id = str(_ci(data, "id", "Id") or "")

    fields: dict[str, str] = {
        "customer_name":              _ci(customer, "name", "Name") or recipient_name or "",
        "recipient_name":             recipient_name,
        "buyer_name":                 buyer_name,
        "to_name":                    _ci(data, "toName", "ToName") or recipient_name or "",
        "from_name":                  _gift_from_name(data, buyer),
        "message":                    _ci(data, "message", "Message") or "",
        "gift_id":                    gift_id,
        "reservation_number":         str(
            _ci(data, "number", "Number", "reservationNumber", "ReservationNumber", "code", "Code") or gift_id or ""
        ),
        "order_code":                 str(_ci(data, "code", "Code") or ""),
        "product_name":               _ci(data, "productName", "ProductName") or "",
        "price_name":                 _ci(data, "priceName", "PriceName") or "",
        "total_price":                str(_ci(data, "totalPrice", "TotalPrice") or ""),
        "redemption_url":             _ci(data, "redemptionUrl", "RedemptionUrl") or "",
        "gift_coupon_code":           _gift_redemption_code(data),
        "gift_theme_name":            _ci(data, "giftThemeName", "GiftThemeName") or "",

        # Formatted date / time (for template params)
        "reservation_date":           _fmt_date(start_dt),
        "start_time":                 _fmt_time(start_dt),
        "end_time":                   _fmt_time(end_dt),

        # Raw ISO strings (for internal scheduling)
        "start_dt_iso":               _fmt_iso(start_raw),
        "end_dt_iso":                 _fmt_iso(end_raw),

        # Invoice — many possible key names from Rekaz
        "invoice_link":               _ci(data, "invoiceUrl", "InvoiceUrl", "invoiceLink", "InvoiceLink", "invoice", "Invoice") or "",

        "header_image_url":           _ci(
            data,
            "giftCardImageUrl",
            "GiftCardImageUrl",
            "imageUrl",
            "ImageUrl",
            "image",
            "Image",
            "productImage",
            "ProductImage",
        ) or "",

        "cancel_reason":              _ci(data, "cancelReason", "CancelReason", "cancellationReason", "CancellationReason") or "",
        # Rekaz actually sends BranchNameAr / BranchNameEn (not BranchName as their docs claim).
        # Prefer Arabic since our customer-facing template is Arabic.
        "branch_name":                _ci(data, "branchNameAr", "BranchNameAr",
                                                "branchNameEn", "BranchNameEn",
                                                "branchName",   "BranchName") or "",

        "reservation_after_minutes":  str(_ci(data, "reservationAfterMinutes", "ReservationAfterMinutes", "afterMinutes", "AfterMinutes") or ""),
        "allowed_late_minutes":       str(_ci(data, "allowedLateMinutes", "AllowedLateMinutes") or ""),
    }

    logger.debug("extract_fields_result", extra={"extra": {"event_name": event_name, "fields": fields}})
    return fields


# ── Spec-driven parameter builder ──────────────────────────────────────

def build_template_parameters(
    template_name: str,
    fields: dict[str, str],
    placeholder: str = "-",
    db: Session | None = None,
) -> list[str]:
    """
    Look up the ordered BODY param spec for *template_name* and return a list
    of values in the correct order.

    Empty strings are replaced with *placeholder* (configurable via
    ``EMPTY_PARAM_PLACEHOLDER``) so Hatif never receives blank body params
    (empty body params cause HTTP 500 from Hatif).
    """
    from app.services.template_catalog import get_template_specs

    specs = get_template_specs(db)
    spec = specs.get(template_name)
    if spec is None:
        spec = TEMPLATE_PARAM_SPECS.get(template_name) or _FALLBACK_SPEC
        logger.warning(
            "template_spec_missing",
            extra={"extra": {"template_name": template_name, "fallback_spec": spec}},
        )

    params = [fields.get(key, "") or placeholder for key in spec]
    logger.info(
        "template_parameters_built",
        extra={
            "extra": {
                "template_name": template_name,
                "spec_keys": spec,
                "param_values": params,
                "param_count": len(params),
            }
        },
    )
    return params


# ── Plain-text message builder  ( for HATIF_SEND_MODE=text) ──────────────

def build_text_message(
    event_name: str,
    customer_name: str | None,
    reservation_number: str | None,
    product_name: str | None,
    start_date: str | None,
) -> str:
    name = customer_name or "عميل"
    number = reservation_number or "-"
    product = product_name or "-"
    date = start_date or "-"

    event_labels = {
        "ReservationCreatedEvent": "تم إنشاء حجز جديد",
        "ReservationConfirmedEvent": "تم تأكيد الحجز",
        "ReservationCancelledEvent": "تم إلغاء الحجز",
        "ReservationReminderEvent": "تذكير بموعد الحجز",
        "ReservationCompletedEvent": "تم اكتمال الحجز",
        "ReservationDoneEvent": "تم إتمام الحجز",
        "ReservationUpdatedEvent": "تم تحديث الحجز",
        "GiftCreatedEvent": "تم إرسال هدية",
        "MerchandiseOrderCreatedEvent": "تم إنشاء طلب منتجات",
    }
    label = event_labels.get(event_name, event_name)

    msg = (
        f"مرحباً {name}،\n"
        f"{label}\n"
        f"رقم الحجز: {number}\n"
        f"المنتج: {product}\n"
        f"تاريخ البدء: {date}"
    )
    logger.info(
        "text_message_built",
        extra={"extra": {"event_name": event_name, "message_length": len(msg)}},
    )
    return msg
