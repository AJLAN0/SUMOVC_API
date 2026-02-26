import logging
import re
from datetime import datetime

logger = logging.getLogger("app.rekaz")

# ── Event → Template mapping ───────────────────────────────────────────
EVENT_TEMPLATE_MAP = {
    "ReservationCreatedEvent": "welcome",
    "ReservationConfirmedEvent": "reservation_confirmed",
    "ReservationCancelledEvent": "reservation_cancelled",
}

# ── Template → ordered BODY parameter names ────────────────────────────
#
#  ⚠️  These MUST match the exact variable count & order in Hatif UI.
#       Sending the wrong number of body params → 500 from Hatif.
#       Empty body params ("") also cause 500 — use EMPTY_PARAM_PLACEHOLDER.
#
TEMPLATE_PARAM_SPECS: dict[str, list[str]] = {
    # client – confirmed / done / updated / completed  (6 body vars)
    "reservation_confirmed": [
        "customer_name",      # {{1}}
        "product_name",       # {{2}}
        "reservation_date",   # {{3}}
        "start_time",         # {{4}}
        "end_time",           # {{5}}
        "invoice_link",       # {{6}}
    ],

    # client – reminder  (3 body vars)
    "reservation_reminder": [
        "customer_name",
        "reservation_after_minutes",
        "allowed_late_minutes",
    ],

    # client – cancelled  (2 body vars)
    "reservation_cancelled": [
        "customer_name",
        "reservation_number",
    ],

    # admin  (4 body vars — matches Hatif UI)
    "admin_reservation_confirmed": [
        "customer_name",
        "product_name",
        "reservation_date",
        "start_time",
    ],

    # welcome (0 body vars)
    "welcome": [],
}

# ── Default fallback spec (if template not in TEMPLATE_PARAM_SPECS) ────
_FALLBACK_SPEC = ["customer_name", "product_name", "reservation_date", "start_time"]


# ── Helpers ─────────────────────────────────────────────────────────────
def map_event_to_template(event_name: str | None) -> str | None:
    if not event_name:
        logger.debug("map_event_to_template called with empty event_name")
        return None
    template = EVENT_TEMPLATE_MAP.get(event_name)
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
    for key in keys:
        for variant in (key, key.lower(), key[0].upper() + key[1:] if key else key):
            if variant in d:
                return d[variant]
    return None


# ── Extract all known fields from Rekaz payload ────────────────────────

def extract_fields(payload: dict) -> dict[str, str]:
    """
    Pull every field the templates might need out of a Rekaz webhook payload
    and return a flat ``{param_name: value}`` dict.

    Dates are parsed and formatted so templates receive clean YYYY-MM-DD / HH:MM.
    Raw ISO datetime strings are also included as ``start_dt_iso`` / ``end_dt_iso``
    for internal use (e.g. scheduling reminders).
    """
    data = payload.get("Data") or payload.get("data") or {}
    customer = data.get("customer") or data.get("Customer") or {}

    # ── Raw datetime strings ──
    start_raw = _ci(data, "startDate", "StartDate", "reservationDate", "ReservationDate")
    end_raw = _ci(data, "endDate", "EndDate")

    start_dt = _parse_dt(start_raw)
    end_dt = _parse_dt(end_raw)

    fields: dict[str, str] = {
        "customer_name":              _ci(customer, "name", "Name") or "",
        "reservation_number":         str(_ci(data, "number", "Number", "reservationNumber", "ReservationNumber") or ""),
        "product_name":               _ci(data, "productName", "ProductName") or "",

        # Formatted date / time (for template params)
        "reservation_date":           _fmt_date(start_dt),
        "start_time":                 _fmt_time(start_dt),
        "end_time":                   _fmt_time(end_dt),

        # Raw ISO strings (for internal scheduling)
        "start_dt_iso":               _fmt_iso(start_raw),
        "end_dt_iso":                 _fmt_iso(end_raw),

        # Invoice — many possible key names from Rekaz
        "invoice_link":               _ci(data, "invoiceUrl", "InvoiceUrl", "invoiceLink", "InvoiceLink", "invoice", "Invoice") or "",

        "cancel_reason":              _ci(data, "cancelReason", "CancelReason", "cancellationReason", "CancellationReason") or "",
        "branch_name":                _ci(data, "branchName", "BranchName") or "",

        "reservation_after_minutes":  str(_ci(data, "reservationAfterMinutes", "ReservationAfterMinutes", "afterMinutes", "AfterMinutes") or ""),
        "allowed_late_minutes":       str(_ci(data, "allowedLateMinutes", "AllowedLateMinutes") or ""),
    }

    logger.debug("extract_fields_result", extra={"extra": {"fields": fields}})
    return fields


# ── Spec-driven parameter builder ──────────────────────────────────────

def build_template_parameters(
    template_name: str,
    fields: dict[str, str],
    placeholder: str = "-",
) -> list[str]:
    """
    Look up the ordered BODY param spec for *template_name* and return a list
    of values in the correct order.

    Empty strings are replaced with *placeholder* (configurable via
    ``EMPTY_PARAM_PLACEHOLDER``) so Hatif never receives blank body params
    (empty body params cause HTTP 500 from Hatif).
    """
    spec = TEMPLATE_PARAM_SPECS.get(template_name)
    if spec is None:
        logger.warning(
            "template_spec_missing",
            extra={"extra": {"template_name": template_name, "fallback_spec": _FALLBACK_SPEC}},
        )
        spec = _FALLBACK_SPEC

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


# ── Plain-text message builder (for HATIF_SEND_MODE=text) ──────────────

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
