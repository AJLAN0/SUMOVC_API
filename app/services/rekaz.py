import logging
import re

logger = logging.getLogger("app.rekaz")

EVENT_TEMPLATE_MAP = {
    "ReservationCreatedEvent": "welcome",
    "ReservationConfirmedEvent": "reservation_confirmed",
    "ReservationCancelledEvent": "reservation_cancelled",
    "ReservationDoneEvent": "reservation_done",
    "ReservationUpdatedEvent": "reservation_updated",
}


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


def build_template_parameters(
    customer_name: str | None,
    reservation_number: str | None,
    product_name: str | None,
    start_date: str | None,
) -> list[str]:
    params = [
        customer_name or "",
        reservation_number or "",
        product_name or "",
        start_date or "",
    ]
    logger.debug(
        "template_parameters_built",
        extra={"extra": {"parameters": params}},
    )
    return params


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