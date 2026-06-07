"""
Rekaz webhook payload classification and routing helpers.

Each Rekaz domain (reservation, gift, merchandise, subscription) uses different
Data shapes and event name prefixes. Dashboard event→template mappings remain
the source of truth for which template to send; this module handles phone,
correlation id, idempotency, and post-send behavior per payload kind.
"""

from __future__ import annotations

import logging
from enum import Enum

logger = logging.getLogger("app.rekaz_payloads")


class PayloadKind(str, Enum):
    RESERVATION = "reservation"
    GIFT = "gift"
    MERCHANDISE = "merchandise"
    SUBSCRIPTION = "subscription"
    UNKNOWN = "unknown"


RESERVATION_CONFIRM_TEMPLATE = "reservation_confirmedddddddd"
RESERVATION_CANCEL_TEMPLATE = "reservation_cancelled"

# Event name prefixes (Rekaz convention)
_PREFIX_RESERVATION = "Reservation"
_PREFIX_GIFT = "Gift"
_PREFIX_MERCHANDISE = "Merchandise"
_PREFIX_SUBSCRIPTION = "Subscription"

RESERVATION_UPDATE_EVENTS = frozenset({"ReservationUpdatedEvent"})

# Default staff routing when DB mapping is missing staff_template_name
STAFF_NOTIFICATION_FALLBACKS: dict[str, tuple[str, str]] = {
    "ReservationConfirmedEvent": ("portrait_technician", "admin_reservation_confirmedddd"),
    "ReservationCreatedEvent": ("portrait_technician", "admin_reservation_confirmedddd"),
    "ReservationUpdatedEvent": ("portrait_technician", "admin_reservation_confirmedddd"),
    "MerchandiseOrderCompletedEvent": ("product_technician", "product_done_admin"),
}


def _ci(d: dict | None, *keys: str):
    if not d:
        return None
    for key in keys:
        for variant in (key, key.lower(), key[0].upper() + key[1:] if key else key):
            if variant in d:
                return d[variant]
    return None


def _gift_shape(data: dict) -> bool:
    return bool(
        _ci(data, "RecipientCustomer", "recipientCustomer")
        or _ci(data, "BuyerCustomer", "buyerCustomer")
        or _ci(data, "RedemptionUrl", "redemptionUrl")
        or _ci(data, "GiftCouponCode", "giftCouponCode")
    )


def _merchandise_shape(data: dict) -> bool:
    code = _ci(data, "code", "Code")
    items = _ci(data, "items", "Items")
    return bool(code and items is not None and not _ci(data, "startDate", "StartDate"))


def _subscription_shape(data: dict) -> bool:
    return bool(
        _ci(data, "PausedAt", "pausedAt") is not None
        or _ci(data, "ResumeAt", "resumeAt") is not None
        or (
            _ci(data, "Name", "name")
            and _ci(data, "Code", "code")
            and _ci(data, "Number", "number")
            and not _ci(data, "productName", "ProductName")
        )
    )


def get_payload_data(payload: dict) -> dict:
    data = payload.get("Data") or payload.get("data")
    return data if isinstance(data, dict) else {}


def classify_payload(event_name: str | None, payload: dict | None = None) -> PayloadKind:
    """Detect payload domain from EventName prefix, with Data-shape fallback."""
    data = get_payload_data(payload or {})

    if event_name:
        if event_name.startswith(_PREFIX_GIFT):
            return PayloadKind.GIFT
        if event_name.startswith(_PREFIX_MERCHANDISE):
            return PayloadKind.MERCHANDISE
        if event_name.startswith(_PREFIX_SUBSCRIPTION):
            return PayloadKind.SUBSCRIPTION
        if event_name.startswith(_PREFIX_RESERVATION):
            return PayloadKind.RESERVATION

    if _gift_shape(data):
        return PayloadKind.GIFT
    if _merchandise_shape(data):
        return PayloadKind.MERCHANDISE
    if _subscription_shape(data):
        return PayloadKind.SUBSCRIPTION
    if _ci(data, "startDate", "StartDate") or _ci(data, "number", "Number"):
        return PayloadKind.RESERVATION

    return PayloadKind.UNKNOWN


def customer_phone_from_object(person: dict | None) -> str | None:
    if not person:
        return None
    return _ci(person, "MobileNumber", "mobileNumber", "phone", "Phone")


def resolve_message_phone(payload: dict, event_name: str | None) -> tuple[str | None, str]:
    """
    Resolve WhatsApp recipient phone by payload kind.
    Gifts → RecipientCustomer; all others → primary Customer/customer.
    """
    kind = classify_payload(event_name, payload)
    data = get_payload_data(payload)

    if kind == PayloadKind.GIFT:
        recipient = data.get("RecipientCustomer") or data.get("recipientCustomer") or {}
        phone = customer_phone_from_object(recipient)
        if phone:
            return phone, "recipient_customer"
        # fallback: buyer if recipient missing (edge case)
        buyer = data.get("BuyerCustomer") or data.get("buyerCustomer") or {}
        return customer_phone_from_object(buyer), "buyer_customer"

    customer = data.get("Customer") or data.get("customer") or {}
    return customer_phone_from_object(customer), "customer"


def entity_id_from_data(data: dict, kind: PayloadKind) -> str:
    raw = _ci(data, "id", "Id") or ""
    return str(raw).strip()


def resolve_correlation_id(fields: dict[str, str], kind: PayloadKind) -> str | None:
    """Stable id for idempotency locks (reservation number, gift id, order code, etc.)."""
    if kind == PayloadKind.GIFT:
        return fields.get("gift_id") or fields.get("entity_id") or fields.get("reservation_number") or None
    if kind == PayloadKind.MERCHANDISE:
        return fields.get("order_code") or fields.get("entity_id") or None
    if kind == PayloadKind.SUBSCRIPTION:
        return (
            fields.get("subscription_number")
            or fields.get("subscription_code")
            or fields.get("entity_id")
            or None
        )
    return fields.get("reservation_number") or fields.get("entity_id") or None


def customer_notification_type(
    event_name: str | None,
    external_event_id: str | None,
    kind: PayloadKind,
) -> str:
    """
    Idempotency key suffix for customer WhatsApp.

    - Reservation confirm: once per reservation+phone
    - Reservation update: once per update webhook
    - Gift / merchandise / subscription / other: once per webhook Id (retries safe)
    """
    if kind == PayloadKind.RESERVATION:
        if event_name in RESERVATION_UPDATE_EVENTS and external_event_id:
            return f"customer_updated:{external_event_id}"
        if event_name in (
            "ReservationConfirmedEvent",
            "ReservationCreatedEvent",
            "ReservationDoneEvent",
        ):
            return "customer_confirmed"
        if external_event_id:
            return f"customer_event:{external_event_id}"

    if external_event_id:
        return f"customer_event:{external_event_id}"
    return f"customer_{kind.value}"


def staff_notification_type(
    event_name: str | None,
    staff_role: str,
    external_event_id: str | None,
    kind: PayloadKind,
) -> str:
    if kind == PayloadKind.RESERVATION and event_name in RESERVATION_UPDATE_EVENTS and external_event_id:
        return f"staff_updated:{staff_role}:{external_event_id}"
    if external_event_id:
        return f"staff_event:{staff_role}:{external_event_id}"
    return f"staff_confirmed:{staff_role}"


def should_schedule_reminder(
    template_name: str | None,
    kind: PayloadKind,
    event_name: str | None,
) -> bool:
    if kind != PayloadKind.RESERVATION:
        return False
    if event_name in ("ReservationCancelledEvent",):
        return False
    return template_name == RESERVATION_CONFIRM_TEMPLATE


def should_cancel_reminders(template_name: str | None, kind: PayloadKind) -> bool:
    return kind == PayloadKind.RESERVATION and template_name == RESERVATION_CANCEL_TEMPLATE


def should_send_staff_for_event(
    event_name: str | None,
    schedule_changed: bool,
) -> bool:
    """Staff alerts follow customer send rules for reservation updates."""
    if event_name in RESERVATION_UPDATE_EVENTS:
        return schedule_changed
    return bool(event_name)


def should_reschedule_reminder_on_update(
    event_name: str | None,
    kind: PayloadKind,
    schedule_changed: bool = True,
) -> bool:
    return (
        kind == PayloadKind.RESERVATION
        and event_name in RESERVATION_UPDATE_EVENTS
        and schedule_changed
    )


def is_gift_kind(kind: PayloadKind) -> bool:
    return kind == PayloadKind.GIFT


def is_reservation_kind(kind: PayloadKind) -> bool:
    return kind == PayloadKind.RESERVATION
