import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("app.config")


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

    # App
    HATIF_SEND_MODE: str = os.getenv("HATIF_SEND_MODE", "template")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./app.db")

    def __post_init__(self) -> None:
        if self.HATIF_SEND_MODE not in {"template", "text"}:
            raise ValueError(
                f"HATIF_SEND_MODE must be 'template' or 'text', got '{self.HATIF_SEND_MODE}'"
            )

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
                    "DATABASE_URL": self.DATABASE_URL,
                }
            },
        )


settings = Settings()
