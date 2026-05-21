"""Turn technical errors into clear Arabic messages for the admin UI."""

from __future__ import annotations

import json
import re
from typing import Any


# Known error patterns → (title, message, hint)
_PATTERNS: list[tuple[re.Pattern[str], str, str, str | None]] = [
    (
        re.compile(r"param_count_mismatch|param count|عدد المتغيرات", re.I),
        "عدد متغيرات القالب غير صحيح",
        "عدد الحقول المُرسلة لا يطابق قالب واتساب في هاتف.",
        "من قسم «قوالب واتساب» تأكد من ترتيب المتغيرات، ثم أعد المحاولة.",
    ),
    (
        re.compile(r"UNIQUE|unique constraint|duplicate|already exists|موجود مسبق", re.I),
        "سجل مكرر",
        "هذا الاسم أو الربط موجود مسبقاً في النظام.",
        "استخدم اسماً مختلفاً أو عدّل السجل الحالي.",
    ),
    (
        re.compile(r"401|unauthorized|not authenticated|غير مصرح", re.I),
        "انتهت الجلسة أو الرفض",
        "لم يتم التحقق من هويتك أو رفض الطلب.",
        "سجّل الدخول من جديد.",
    ),
    (
        re.compile(r"403|forbidden", re.I),
        "صلاحية مرفوضة",
        "لا تملك صلاحية تنفيذ هذا الإجراء.",
        None,
    ),
    (
        re.compile(r"404|not found", re.I),
        "غير موجود",
        "العنصر المطلوب غير موجود (ربما حُذف).",
        "حدّث الصفحة وحاول مرة أخرى.",
    ),
    (
        re.compile(r"template.*not found|لم يُعثر على.*قالب", re.I),
        "قالب غير معرّف",
        "اسم القالب غير موجود في لوحة التحكم أو غير مفعّل.",
        "أضف القالب من «قوالب واتساب» بنفس الاسم في هاتف.",
    ),
    (
        re.compile(r"invalid.*json|JSONDecodeError|تعذر.*JSON", re.I),
        "بيانات غير صالحة",
        "صيغة البيانات غير صحيحة.",
        None,
    ),
    (
        re.compile(r"connection|timeout|refused|unreachable|شبكة", re.I),
        "مشكلة اتصال",
        "تعذر الاتصال بالخادم أو بمزوّد واتساب.",
        "تحقق من الإنترنت وإعدادات HATIF في صفحة النظام.",
    ),
    (
        re.compile(r"token|credential|client_secret|access.?token|مصادقة", re.I),
        "فشل مصادقة هاتف",
        "بيانات اعتماد هاتف (Voxa) غير صحيحة أو منتهية.",
        "راجع HATIF_CLIENT_ID و HATIF_CLIENT_SECRET في Railway.",
    ),
    (
        re.compile(r"500|internal server|hatif.*500|body param", re.I),
        "رفض من مزوّد واتساب",
        "هاتف رفض الرسالة — غالباً حقل فارغ أو قالب غير مطابق.",
        "تأكد من ملء كل متغيرات القالب؛ الفارغ يُستبدل بـ «-» تلقائياً.",
    ),
    (
        re.compile(r"mapping|ربط", re.I),
        "خطأ في ربط الحدث",
        "تعذر حفظ ربط الحدث بالقالب.",
        "تأكد أن اسم الحدث مطابق لركاز والقالب مفعّل.",
    ),
    (
        re.compile(r"rate.?limit|429|محاولات كثيرة", re.I),
        "محاولات كثيرة",
        "تم إيقاف المحاولات مؤقتاً لحماية النظام.",
        "انتظر ١٥ دقيقة ثم حاول.",
    ),
]


def explain_error(raw: str | None) -> dict[str, str | None]:
    """Return {title, message, hint, raw} with Arabic-friendly text."""
    text = (raw or "").strip()
    if not text:
        return {
            "title": "خطأ",
            "message": "حدث خطأ غير معروف.",
            "hint": None,
            "raw": None,
        }

    for pattern, title, message, hint in _PATTERNS:
        if pattern.search(text):
            return {"title": title, "message": message, "hint": hint, "raw": text}

    # Short technical messages — show as-is with wrapper
    if len(text) <= 120 and not text.startswith("{"):
        return {
            "title": "تفاصيل الخطأ",
            "message": text,
            "hint": "إذا تكرّر الخطأ، راجع سجل الرسائل أو تواصل مع الدعم الفني.",
            "raw": text,
        }

    return {
        "title": "خطأ تقني",
        "message": text[:200] + ("…" if len(text) > 200 else ""),
        "hint": "النص الكامل متاح في تفاصيل السجل.",
        "raw": text,
    }


def humanize_error(raw: str | None) -> str:
    """One-line summary for tables (Jinja filter)."""
    ex = explain_error(raw)
    msg = ex["message"] or ""
    if ex["hint"]:
        return msg
    return msg


def humanize_error_block(raw: str | None) -> dict[str, str | None]:
    """Full block for detail pages."""
    return explain_error(raw)


def format_api_error(status_code: int, detail: Any) -> dict[str, Any]:
    """JSON body for admin API errors."""
    raw = _detail_to_str(detail)
    ex = explain_error(raw)
    return {
        "ok": False,
        "status": status_code,
        "title": ex["title"],
        "message_ar": ex["message"],
        "hint": ex["hint"],
        "detail": raw,
    }


def _detail_to_str(detail: Any) -> str:
    if detail is None:
        return ""
    if isinstance(detail, dict):
        return (
            detail.get("message_ar")
            or detail.get("message")
            or detail.get("detail")
            or json.dumps(detail, ensure_ascii=False)
        )
    if isinstance(detail, list):
        parts = []
        for item in detail[:3]:
            if isinstance(item, dict):
                loc = ".".join(str(x) for x in item.get("loc", []))
                msg = item.get("msg", "")
                parts.append(f"{loc}: {msg}" if loc else msg)
            else:
                parts.append(str(item))
        return " · ".join(parts) or "خطأ في التحقق من البيانات"
    return str(detail)


def validate_phone(phone: str) -> str | None:
    """Return Arabic error message or None if OK."""
    p = (phone or "").strip().replace(" ", "").replace("+", "")
    if not p:
        return "أدخل رقم الجوال."
    if not p.isdigit():
        return "رقم الجوال يجب أن يحتوي على أرقام فقط (مثال: 9665XXXXXXXX)."
    if len(p) < 10 or len(p) > 15:
        return "طول رقم الجوال غير صحيح."
    if not p.startswith("966"):
        return "يجب أن يبدأ الرقم بـ 966 (سعودي)."
    return None
