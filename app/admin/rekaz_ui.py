"""Dashboard helpers for Rekaz payload kinds and event configuration."""

from __future__ import annotations

from typing import Any

from app.services.rekaz_payloads import PayloadKind, classify_payload

PAYLOAD_KIND_LABELS_AR: dict[PayloadKind, str] = {
    PayloadKind.RESERVATION: "حجز",
    PayloadKind.GIFT: "هدية",
    PayloadKind.MERCHANDISE: "منتجات",
    PayloadKind.SUBSCRIPTION: "اشتراك",
    PayloadKind.UNKNOWN: "غير معروف",
}

PHONE_HINTS_AR: dict[PayloadKind, str] = {
    PayloadKind.RESERVATION: "يُرسل إلى customer.MobileNumber",
    PayloadKind.GIFT: "يُرسل إلى RecipientCustomer (المستلم)",
    PayloadKind.MERCHANDISE: "يُرسل إلى Customer",
    PayloadKind.SUBSCRIPTION: "يُرسل إلى Customer (phone أو MobileNumber)",
    PayloadKind.UNKNOWN: "يُحدد حسب شكل الحمولة",
}

# Template variable hints grouped by payload domain (for dashboard reference)
FIELDS_BY_KIND: dict[PayloadKind, list[str]] = {
    PayloadKind.RESERVATION: [
        "customer_name",
        "product_name",
        "reservation_date",
        "start_time",
        "end_time",
        "branch_name",
        "reservation_number",
        "cancel_reason",
    ],
    PayloadKind.GIFT: [
        "recipient_name",
        "from_name",
        "message",
        "product_name",
        "redemption_url",
        "gift_coupon_code",
        "to_name",
        "buyer_name",
    ],
    PayloadKind.MERCHANDISE: [
        "customer_name",
        "order_code",
        "product_name",
        "items_summary",
        "total_price",
        "discount",
        "branch_name",
    ],
    PayloadKind.SUBSCRIPTION: [
        "customer_name",
        "subscription_number",
        "subscription_code",
        "product_name",
        "reservation_date",
        "start_time",
        "end_date",
        "total_price",
        "branch_name",
    ],
}

PAYLOAD_KIND_ORDER: list[PayloadKind] = [
    PayloadKind.RESERVATION,
    PayloadKind.GIFT,
    PayloadKind.MERCHANDISE,
    PayloadKind.SUBSCRIPTION,
    PayloadKind.UNKNOWN,
]


def payload_kind_for_event(event_name: str | None) -> PayloadKind:
    return classify_payload(event_name)


def kind_label(kind: PayloadKind) -> str:
    return PAYLOAD_KIND_LABELS_AR.get(kind, kind.value)


def mapping_row_context(mapping: Any) -> dict[str, Any]:
    kind = payload_kind_for_event(mapping.event_name)
    return {
        "mapping": mapping,
        "payload_kind": kind.value,
        "kind_label": kind_label(kind),
        "phone_hint": PHONE_HINTS_AR.get(kind, ""),
    }


def build_event_groups(seed_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group known Rekaz event names by payload kind for dashboard selects."""
    buckets: dict[PayloadKind, list[str]] = {k: [] for k in PAYLOAD_KIND_ORDER}
    seen: set[str] = set()
    for seed in seed_events:
        name = (seed.get("event_name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        kind = payload_kind_for_event(name)
        buckets[kind].append(name)
    for kind in buckets:
        buckets[kind].sort()
    return [
        {
            "kind": kind.value,
            "kind_label": kind_label(kind),
            "phone_hint": PHONE_HINTS_AR.get(kind, ""),
            "events": buckets[kind],
        }
        for kind in PAYLOAD_KIND_ORDER
        if buckets[kind]
    ]


def filter_mappings_by_kind(items: list[Any], kind_filter: str | None) -> list[Any]:
    if not kind_filter or kind_filter == "all":
        return items
    try:
        target = PayloadKind(kind_filter)
    except ValueError:
        return items
    return [m for m in items if payload_kind_for_event(m.event_name) == target]
