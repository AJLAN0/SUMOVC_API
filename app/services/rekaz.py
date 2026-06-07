import logging
import re
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.services.rekaz_payloads import (
    PayloadKind,
    RESERVATION_CONFIRM_TEMPLATE,
    classify_payload,
    customer_notification_type,
    entity_id_from_data,
    get_payload_data,
    is_gift_kind,
    is_reservation_kind,
    resolve_correlation_id as resolve_correlation_id_for_kind,
    resolve_message_phone,
    staff_notification_type,
    should_cancel_reminders,
    should_reschedule_reminder_on_update,
    should_schedule_reminder,
    should_send_staff_for_event,
)

logger = logging.getLogger("app.rekaz")

# Rekaz reservation times are Saudi local; API often tags them as +00:00 or Z (not true UTC).
_RIYADH = timezone(timedelta(hours=3))

# ── Event → Template mapping ───────────────────────────────────────────
EVENT_TEMPLATE_MAP = {
    "ReservationConfirmedEvent": "reservation_confirmedddddddd",
    "ReservationUpdatedEvent": "reservation_confirmedddddddd",
    "ReservationCancelledEvent": "reservation_cancelled",
    "GiftCreatedEvent": "gift_clint_send",
    "MerchandiseOrderCompletedEvent": "product_clint_done",
}

# Re-export payload helpers (webhook + tests)
__all__ = [
    "PayloadKind",
    "RESERVATION_CONFIRM_TEMPLATE",
    "classify_payload",
    "customer_notification_type",
    "extract_fields",
    "is_gift_event",
    "is_gift_kind",
    "is_merchandise_event",
    "is_reservation_kind",
    "is_reservation_update_event",
    "load_previous_reservation_fields",
    "reservation_schedule_changed",
    "schedule_snapshot",
    "RESERVATION_SCHEDULE_FIELD_KEYS",
    "map_event_to_template",
    "resolve_correlation_id",
    "resolve_message_phone",
    "resolve_template_language",
    "should_cancel_reminders",
    "should_reschedule_reminder_on_update",
    "should_schedule_reminder",
    "should_send_staff_for_event",
    "staff_notification_type",
]

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

    # gift – sent to RecipientCustomer (5 body vars — Hatif template gift_clint_send)
    "gift_clint_send": [
        "recipient_name",     # {{1}} recipient name
        "from_name",          # {{2}} sender name
        "message",            # {{3}} gift message
        "product_name",       # {{4}} product / gift name
        "redemption_url",     # {{5}} redemption link
    ],
    # legacy aliases → same spec
    "gifft_send": [
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

    # merchandise – order completed customer (1 body var — Hatif template product_clint_done)
    "product_clint_done": [
        "product_name",       # {{1}} item / product name
    ],
    # legacy alias
    "product_done_clint": [
        "product_name",
    ],

    # merchandise – order completed staff (2 body vars — Hatif template product_done_admin, English)
    "product_done_admin": [
        "customer_name",      # {{1}} Clint name
        "product_name",       # {{2}} product name
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
    return classify_payload(event_name) == PayloadKind.GIFT


def is_merchandise_event(event_name: str | None) -> bool:
    return classify_payload(event_name) == PayloadKind.MERCHANDISE


def is_reservation_update_event(event_name: str | None) -> bool:
    from app.services.rekaz_payloads import RESERVATION_UPDATE_EVENTS

    return bool(event_name and event_name in RESERVATION_UPDATE_EVENTS)


RESERVATION_SCHEDULE_FIELD_KEYS = ("start_dt_iso", "end_dt_iso", "reservation_date")


def _normalize_schedule_date(fields: dict[str, str]) -> str:
    """Canonical YYYY-MM-DD for schedule comparison."""
    raw = (fields.get("reservation_date") or "").strip()
    if raw:
        parsed = _parse_dt(raw)
        if parsed:
            return parsed.strftime("%Y-%m-%d")
        if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
            return raw[:10]
        return raw
    start_utc = rekaz_start_to_utc(fields.get("start_dt_iso") or None)
    if start_utc:
        return start_utc.strftime("%Y-%m-%d")
    return ""


def _normalize_schedule_instant(fields: dict[str, str], iso_key: str) -> str:
    """Canonical UTC wall clock YYYY-MM-DD HH:MM for schedule comparison."""
    raw = (fields.get(iso_key) or "").strip()
    if not raw:
        return ""
    dt = rekaz_start_to_utc(raw)
    if dt:
        return dt.strftime("%Y-%m-%d %H:%M")
    parsed = _parse_dt(raw)
    if parsed:
        return parsed.strftime("%Y-%m-%d %H:%M")
    return raw


def schedule_snapshot(fields: dict[str, str]) -> tuple[str, str, str]:
    """Normalized (start, end, date) tuple — ignores formatting-only differences."""
    return (
        _normalize_schedule_instant(fields, "start_dt_iso"),
        _normalize_schedule_instant(fields, "end_dt_iso"),
        _normalize_schedule_date(fields),
    )


def load_previous_reservation_fields(
    db: Session,
    reservation_number: str | None,
    exclude_external_event_id: str | None,
) -> dict[str, str] | None:
    """Most recent reservation webhook for the same booking, excluding the current event."""
    import json

    from sqlalchemy import select

    from app.models import WebhookEvent

    if not reservation_number:
        return None

    rows = db.execute(
        select(WebhookEvent)
        .where(
            WebhookEvent.event_name.like("Reservation%"),
            WebhookEvent.payload_json.contains(reservation_number),
        )
        .order_by(WebhookEvent.created_at.desc())
        .limit(30)
    ).scalars().all()

    for row in rows:
        if exclude_external_event_id and row.external_event_id == exclude_external_event_id:
            continue
        try:
            payload = json.loads(row.payload_json)
        except json.JSONDecodeError:
            continue
        prev = extract_fields(payload, row.event_name)
        if prev.get("reservation_number") == reservation_number:
            return prev
    return None


def reservation_schedule_changed(
    current_fields: dict[str, str],
    previous_fields: dict[str, str] | None,
) -> bool:
    """True when start/end datetime or reservation date meaningfully differs."""
    if not previous_fields:
        return False

    cur = schedule_snapshot(current_fields)
    prev = schedule_snapshot(previous_fields)

    # Start time changed
    if cur[0] != prev[0] and (cur[0] or prev[0]):
        return True

    # Reservation date changed
    if cur[2] != prev[2] and (cur[2] or prev[2]):
        return True

    # End time changed — only when both payloads include an end time (Rekaz often
    # adds EndDate on updates even when the booking end did not actually change).
    if cur[1] and prev[1] and cur[1] != prev[1]:
        return True

    return False


def resolve_correlation_id(fields: dict[str, str], event_name: str | None = None) -> str | None:
    kind_str = fields.get("payload_kind")
    try:
        kind = PayloadKind(kind_str) if kind_str else classify_payload(event_name)
    except ValueError:
        kind = classify_payload(event_name)
    return resolve_correlation_id_for_kind(fields, kind)


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
    return str(buyer_name).strip() if buyer_name else "-"


def _merchandise_items_summary(data: dict) -> str:
    items = _ci(data, "items", "Items") or []
    if not isinstance(items, list) or not items:
        return ""
    names: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        label = (
            _ci(item, "ProductName", "productName")
            or _ci(item, "Name", "name")
            or _ci(item, "PriceName", "priceName")
            or ""
        )
        qty = _ci(item, "Quantity", "quantity")
        if label and qty:
            names.append(f"{label} x{qty}")
        elif label:
            names.append(str(label))
    return "، ".join(names)


def resolve_template_language(payload: dict, event_name: str | None, default: str) -> str:
    data = get_payload_data(payload)
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
    data = get_payload_data(payload)
    kind = classify_payload(event_name, payload)

    buyer = data.get("BuyerCustomer") or data.get("buyerCustomer") or {}
    recipient = data.get("RecipientCustomer") or data.get("recipientCustomer") or {}
    customer = data.get("Customer") or data.get("customer") or {}

    if kind == PayloadKind.GIFT:
        customer = recipient or customer
    elif kind == PayloadKind.SUBSCRIPTION:
        customer = data.get("Customer") or data.get("customer") or customer

    formatted_from_date = _ci(data, "formattedFromDate", "FormattedFromDate")
    formatted_from_time = _ci(data, "formattedFromTime", "FormattedFromTime")
    formatted_end_date = _ci(data, "formattedEndDate", "FormattedEndDate")
    formatted_end_time = _ci(data, "formattedEndTime", "FormattedEndTime")

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

    entity_id = entity_id_from_data(data, kind)
    recipient_name = _ci(recipient, "name", "Name") or _ci(data, "toName", "ToName") or ""
    buyer_name = _ci(buyer, "name", "Name") or _ci(data, "fromName", "FromName") or ""

    product_name = (
        _ci(data, "productName", "ProductName")
        or _ci(data, "Name", "name")
        or _merchandise_items_summary(data)
        or ""
    )

    reservation_number = str(
        _ci(data, "number", "Number", "reservationNumber", "ReservationNumber") or entity_id or ""
    )
    order_code = str(_ci(data, "code", "Code") or "")
    subscription_number = str(_ci(data, "number", "Number") or "")
    subscription_code = str(_ci(data, "code", "Code") or "")

    if kind == PayloadKind.GIFT and not reservation_number:
        reservation_number = entity_id
    if kind == PayloadKind.MERCHANDISE and not reservation_number:
        reservation_number = order_code or entity_id
    if kind == PayloadKind.SUBSCRIPTION and not reservation_number:
        reservation_number = subscription_number or subscription_code or entity_id

    fields: dict[str, str] = {
        "customer_name":              _ci(customer, "name", "Name") or recipient_name or "",
        "recipient_name":             recipient_name,
        "buyer_name":                 buyer_name,
        "to_name":                    _ci(data, "toName", "ToName") or recipient_name or "",
        "from_name":                  _gift_from_name(data, buyer) if kind == PayloadKind.GIFT else (_ci(data, "fromName", "FromName") or buyer_name or ""),
        "message":                    _ci(data, "message", "Message") or "",
        "entity_id":                  entity_id,
        "gift_id":                    entity_id if kind == PayloadKind.GIFT else "",
        "reservation_number":         reservation_number,
        "order_code":                 order_code,
        "subscription_number":        subscription_number,
        "subscription_code":          subscription_code,
        "product_name":               product_name,
        "price_name":                 _ci(data, "priceName", "PriceName", "OptionName", "optionName") or "",
        "total_price":                str(_ci(data, "totalPrice", "TotalPrice", "price", "Price") or ""),
        "discount":                   str(_ci(data, "discount", "Discount") or ""),
        "status":                     str(_ci(data, "status", "Status") or ""),
        "redemption_url":             _ci(data, "redemptionUrl", "RedemptionUrl") or "",
        "gift_coupon_code":           _gift_redemption_code(data),
        "gift_theme_name":            _ci(data, "giftThemeName", "GiftThemeName") or "",
        "items_summary":              _merchandise_items_summary(data),

        "reservation_date":           formatted_from_date or _fmt_date(start_dt),
        "start_time":                 formatted_from_time or _fmt_time(start_dt),
        "end_time":                   formatted_end_time or _fmt_time(end_dt),
        "end_date":                   formatted_end_date or _fmt_date(end_dt),

        "start_dt_iso":               _fmt_iso(start_raw),
        "end_dt_iso":                 _fmt_iso(end_raw),

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
        "branch_name":                _ci(data, "branchNameAr", "BranchNameAr",
                                                "branchNameEn", "BranchNameEn",
                                                "branchName",   "BranchName") or "",

        "reservation_after_minutes":  str(_ci(data, "reservationAfterMinutes", "ReservationAfterMinutes", "afterMinutes", "AfterMinutes") or ""),
        "allowed_late_minutes":       str(_ci(data, "allowedLateMinutes", "AllowedLateMinutes") or ""),
        "payload_kind":               kind.value,
    }

    logger.debug(
        "extract_fields_result",
        extra={"extra": {"event_name": event_name, "payload_kind": kind.value, "fields": fields}},
    )
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
        "MerchandiseOrderCompletedEvent": "تم تأكيد شراء المنتجات",
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
