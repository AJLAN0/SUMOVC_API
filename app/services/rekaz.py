import logging
import re

logger = logging.getLogger("app.rekaz")

# ── Event → Template mapping ───────────────────────────────────────────
EVENT_TEMPLATE_MAP = {
    # Client-facing
    "ReservationCreatedEvent": "welcome",
    "ReservationConfirmedEvent": "reservation_confirmed",
    "ReservationCancelledEvent": "reservation_cancelled",
    "ReservationReminderEvent": "reservation_reminder",
    "ReservationCompletedEvent": "reservation_confirmed",   # same template
    "ReservationDoneEvent": "reservation_confirmed",        # same template
    "ReservationUpdatedEvent": "reservation_confirmed",     # same template

    # Admin-facing
    "AdminReservationCreatedEvent": "admin_reservation_confirmed",
    "AdminReservationConfirmedEvent": "admin_reservation_confirmed",
    "AdminReservationCancelledEvent": "reservation_cancelled",
}

# ── Template → ordered parameter names ─────────────────────────────────
TEMPLATE_PARAM_SPECS: dict[str, list[str]] = {
    # client – confirmed / done / updated / completed
    "reservation_confirmed": [
        "customer_name",
        "product_name",
        "reservation_date",
        "start_time",
        "end_time",
        "location_link",
        "location_text",
        "important_notes",
        "invoice_link",
        "meeting_link",
    ],

    # client – reminder
    "reservation_reminder": [
        "customer_name",
        "reservation_after_minutes",
        "allowed_late_minutes",
    ],

    # client – cancelled
    "reservation_cancelled": [
        "customer_name",
        "reservation_number",
        "cancel_reason",
    ],

    # admin
    "admin_reservation_confirmed": [
        "customer_name",
        "product_name",
        "reservation_date",
        "start_time",
        "end_time",
        "branch_name",
        "reservation_number",
    ],

    # welcome (no body params)
    "welcome": [],
}


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


# ── Extract all known fields from Rekaz payload ────────────────────────

def _ci(d: dict, *keys: str):
    """Case-insensitive dict lookup – tries each key as-is, then lower, then title."""
    for key in keys:
        for variant in (key, key.lower(), key[0].upper() + key[1:] if key else key):
            if variant in d:
                return d[variant]
    return None


def extract_fields(payload: dict) -> dict[str, str]:
    """
    Pull every field the templates might need out of a Rekaz webhook payload
    and return a flat ``{param_name: value}`` dict.
    """
    data = payload.get("Data") or payload.get("data") or {}
    customer = data.get("customer") or data.get("Customer") or {}

    fields: dict[str, str] = {
        "customer_name":              _ci(customer, "name", "Name") or "",
        "reservation_number":         str(_ci(data, "number", "Number", "reservationNumber", "ReservationNumber") or ""),
        "product_name":               _ci(data, "productName", "ProductName") or "",
        "reservation_date":           _ci(data, "startDate", "StartDate", "reservationDate", "ReservationDate") or "",
        "start_time":                 _ci(data, "startTime", "StartTime", "startDate", "StartDate") or "",
        "end_time":                   _ci(data, "endTime", "EndTime", "endDate", "EndDate") or "",
        "location_link":              _ci(data, "locationLink", "LocationLink") or "",
        "location_text":              _ci(data, "locationText", "LocationText", "location", "Location") or "",
        "important_notes":            _ci(data, "importantNotes", "ImportantNotes", "notes", "Notes") or "",
        "invoice_link":               _ci(data, "invoiceLink", "InvoiceLink") or "",
        "meeting_link":               _ci(data, "meetingLink", "MeetingLink") or "",
        "reservation_after_minutes":  str(_ci(data, "reservationAfterMinutes", "ReservationAfterMinutes", "afterMinutes") or ""),
        "allowed_late_minutes":       str(_ci(data, "allowedLateMinutes", "AllowedLateMinutes") or ""),
        "cancel_reason":              _ci(data, "cancelReason", "CancelReason", "cancellationReason") or "",
        "branch_name":                _ci(data, "branchName", "BranchName") or "",
    }

    logger.debug("extract_fields_result", extra={"extra": {"fields": fields}})
    return fields


# ── Spec-driven parameter builder ──────────────────────────────────────

def build_template_parameters(template_name: str, fields: dict[str, str]) -> list[str]:
    """
    Given a template name, look up the ordered param spec and return a list
    of values in the correct order (empty string for any missing field).
    """
    spec = TEMPLATE_PARAM_SPECS.get(template_name, [])
    params = [fields.get(key, "") for key in spec]
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
