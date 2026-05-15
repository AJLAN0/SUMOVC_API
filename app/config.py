import logging
import os
import secrets
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("app.config")

def _hash_password(plain: str) -> str:
    import bcrypt

    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(plain: str, hashed: str) -> bool:
    import bcrypt

    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def _must(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


@dataclass(frozen=True)
class Settings:
    # Rekaz webhook auth
    REKAZ_BASIC_AUTH: str = _must("REKAZ_BASIC_AUTH")
    REKAZ_TENANT_ID: str = _must("REKAZ_TENANT_ID")

    # Hatif
    HATIF_BASE_URL: str = os.getenv("HATIF_BASE_URL", "https://api.voxa.sa")
    HATIF_CLIENT_ID: str = _must("HATIF_CLIENT_ID")
    HATIF_CLIENT_SECRET: str = _must("HATIF_CLIENT_SECRET")
    HATIF_SCOPE: str = os.getenv("HATIF_SCOPE", "VoxaAPI")
    HATIF_CHANNEL_ID: str = _must("HATIF_CHANNEL_ID")
    HATIF_WEBHOOK_SECRET: str = os.getenv("HATIF_WEBHOOK_SECRET", "")

    # Template sending
    HATIF_TEMPLATE_LANGUAGE: str = os.getenv("HATIF_TEMPLATE_LANGUAGE", "ar")
    EMPTY_PARAM_PLACEHOLDER: str = os.getenv("EMPTY_PARAM_PLACEHOLDER", "-")

    # Default header image for conf_clint template (used when Rekaz payload has no image)
    #CONF_CLINT_HEADER_IMAGE: str = os.getenv("CONF_CLINT_HEADER_IMAGE", "")

    # Admin / Reminder
    ADMIN_TO_NUMBERS: str = os.getenv("ADMIN_TO_NUMBERS", "")  # "9665xxxxxxx,9665yyyyyyy"
    REMINDER_BEFORE_MINUTES: int = int(os.getenv("REMINDER_BEFORE_MINUTES") or "20")
    ALLOWED_LATE_MINUTES: int = int(os.getenv("ALLOWED_LATE_MINUTES") or "10")

    # App
    HATIF_SEND_MODE: str = os.getenv("HATIF_SEND_MODE", "template")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./app.db")

    # Admin dashboard
    ADMIN_EMAIL: str = os.getenv("ADMIN_EMAIL", "").strip().lower()
    ADMIN_PASSWORD_HASH: str = os.getenv("ADMIN_PASSWORD_HASH", "").strip()
    ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "").strip()
    ADMIN_SESSION_SECRET: str = os.getenv("ADMIN_SESSION_SECRET", "").strip()
    ADMIN_COOKIE_SECURE: bool = os.getenv("ADMIN_COOKIE_SECURE", "").lower() in ("1", "true", "yes")

    _resolved_admin_password_hash: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if self.HATIF_SEND_MODE not in {"template", "text"}:
            raise ValueError(
                f"HATIF_SEND_MODE must be 'template' or 'text', got '{self.HATIF_SEND_MODE}'"
            )
        object.__setattr__(self, "_resolved_admin_password_hash", self._resolve_admin_password_hash())

    def _resolve_admin_password_hash(self) -> str:
        if self.ADMIN_PASSWORD_HASH:
            return self.ADMIN_PASSWORD_HASH
        if self.ADMIN_PASSWORD:
            hashed = _hash_password(self.ADMIN_PASSWORD)
            logger.warning(
                "admin_password_hashed_from_plain_env",
                extra={"extra": {"hint": "Set ADMIN_PASSWORD_HASH on Railway and remove ADMIN_PASSWORD"}},
            )
            return hashed
        return ""

    def admin_configured(self) -> bool:
        return bool(
            self.ADMIN_EMAIL
            and self._resolved_admin_password_hash
            and self.ADMIN_SESSION_SECRET
        )

    def verify_admin_password(self, plain: str) -> bool:
        if not self._resolved_admin_password_hash:
            return False
        return _verify_password(plain, self._resolved_admin_password_hash)

    def effective_session_secret(self) -> str:
        if self.ADMIN_SESSION_SECRET:
            return self.ADMIN_SESSION_SECRET
        return secrets.token_hex(32)

    def admin_numbers(self) -> list[str]:
        """Return parsed + phone-normalized list of admin numbers."""
        from app.services.rekaz import normalize_phone

        raw = self.ADMIN_TO_NUMBERS.strip()
        if not raw:
            return []
        numbers = [x.strip() for x in raw.split(",") if x.strip()]
        return [n for n in (normalize_phone(x) for x in numbers) if n]

    def log_summary(self) -> None:
        """Log a safe summary of loaded settings (secrets masked)."""
        logger.info(
            "settings_loaded",
            extra={
                "extra": {
                    "REKAZ_TENANT_ID": self.REKAZ_TENANT_ID,
                    "REKAZ_BASIC_AUTH": f"{self.REKAZ_BASIC_AUTH[:4]}****" if len(self.REKAZ_BASIC_AUTH) > 4 else "****",
                    "HATIF_BASE_URL": self.HATIF_BASE_URL,
                    "HATIF_CLIENT_ID": self.HATIF_CLIENT_ID,
                    "HATIF_CLIENT_SECRET": f"{self.HATIF_CLIENT_SECRET[:4]}****" if len(self.HATIF_CLIENT_SECRET) > 4 else "****",
                    "HATIF_SCOPE": self.HATIF_SCOPE,
                    "HATIF_CHANNEL_ID": self.HATIF_CHANNEL_ID,
                    "HATIF_WEBHOOK_SECRET": "set" if self.HATIF_WEBHOOK_SECRET else "empty",
                    "HATIF_SEND_MODE": self.HATIF_SEND_MODE,
                    "HATIF_TEMPLATE_LANGUAGE": self.HATIF_TEMPLATE_LANGUAGE,
                    "EMPTY_PARAM_PLACEHOLDER": self.EMPTY_PARAM_PLACEHOLDER,
                    "DATABASE_URL": self.DATABASE_URL,
                    "ADMIN_TO_NUMBERS": self.ADMIN_TO_NUMBERS or "(none)",
                    "REMINDER_BEFORE_MINUTES": self.REMINDER_BEFORE_MINUTES,
                    "ALLOWED_LATE_MINUTES": self.ALLOWED_LATE_MINUTES,
                    "ADMIN_EMAIL": self.ADMIN_EMAIL or "(not set)",
                    "ADMIN_CONFIGURED": self.admin_configured(),
                }
            },
        )

    def admin_settings_masked(self) -> dict:
        """Safe dict for admin system page."""
        return {
            "REKAZ_TENANT_ID": self.REKAZ_TENANT_ID,
            "REKAZ_BASIC_AUTH": f"{self.REKAZ_BASIC_AUTH[:4]}****" if len(self.REKAZ_BASIC_AUTH) > 4 else "****",
            "HATIF_BASE_URL": self.HATIF_BASE_URL,
            "HATIF_CLIENT_ID": self.HATIF_CLIENT_ID,
            "HATIF_CLIENT_SECRET": f"{self.HATIF_CLIENT_SECRET[:4]}****"
            if len(self.HATIF_CLIENT_SECRET) > 4
            else "****",
            "HATIF_SCOPE": self.HATIF_SCOPE,
            "HATIF_CHANNEL_ID": self.HATIF_CHANNEL_ID,
            "HATIF_WEBHOOK_SECRET": "set" if self.HATIF_WEBHOOK_SECRET else "empty",
            "HATIF_SEND_MODE": self.HATIF_SEND_MODE,
            "HATIF_TEMPLATE_LANGUAGE": self.HATIF_TEMPLATE_LANGUAGE,
            "EMPTY_PARAM_PLACEHOLDER": self.EMPTY_PARAM_PLACEHOLDER,
            "DATABASE_URL": self._mask_db_url(self.DATABASE_URL),
            "ADMIN_TO_NUMBERS": self.ADMIN_TO_NUMBERS or "(none)",
            "REMINDER_BEFORE_MINUTES": self.REMINDER_BEFORE_MINUTES,
            "ALLOWED_LATE_MINUTES": self.ALLOWED_LATE_MINUTES,
            "ADMIN_EMAIL": self.ADMIN_EMAIL or "(not set)",
        }

    @staticmethod
    def _mask_db_url(url: str) -> str:
        try:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            if parsed.password:
                return url.replace(parsed.password, "****")
        except Exception:
            pass
        return url


settings = Settings()
