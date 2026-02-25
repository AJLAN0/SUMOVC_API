import logging
from urllib.parse import urlparse

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import settings

logger = logging.getLogger("app.database")


def _safe_db_url(url: str) -> str:
    """Return URL with password masked for logging."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            return url.replace(parsed.password, "****")
    except Exception:
        pass
    return url


def _normalize_database_url(url: str) -> str:
    """
    Normalize DB URL for SQLAlchemy:
    - Fail fast if empty.
    - Ensure PostgreSQL uses a known driver (psycopg recommended).
    """
    if not url or not url.strip():
        raise RuntimeError("DATABASE_URL is empty. Set DATABASE_URL in environment variables.")

    url = url.strip()

    # If someone provided 'postgres://', normalize to 'postgresql://'
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]

    # Prefer psycopg driver explicitly for SQLAlchemy 2.x
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://"):]

    return url


DATABASE_URL = _normalize_database_url(settings.DATABASE_URL)

is_sqlite = DATABASE_URL.startswith("sqlite")
is_postgres = DATABASE_URL.startswith("postgresql+psycopg://") or DATABASE_URL.startswith("postgresql://")

connect_args = {}
engine_kwargs = {
    "future": True,
    "pool_pre_ping": True,       # Avoid stale connections (common on cloud/proxy)
    "pool_recycle": 300,         # Recycle connections every 5 minutes
}

if is_sqlite:
    connect_args = {"check_same_thread": False}
else:
    # For Postgres (Railway public proxy), default connect args are OK.
    # If you enable SSL and Railway requires it, uncomment:
    # connect_args = {"sslmode": "require"}
    connect_args = {}

engine = create_engine(DATABASE_URL, connect_args=connect_args, **engine_kwargs)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
Base = declarative_base()

logger.info(
    "database_engine_created",
    extra={
        "extra": {
            "url": _safe_db_url(DATABASE_URL),
            "backend": "sqlite" if is_sqlite else ("postgres" if is_postgres else "other"),
        }
    },
)


def get_db():
    db = SessionLocal()
    logger.debug("db_session_opened")
    try:
        yield db
    finally:
        db.close()
        logger.debug("db_session_closed")


def init_db() -> None:
    logger.info("init_db_started")
    Base.metadata.create_all(bind=engine)
    logger.info("init_db_tables_created")

    # SQLite-only best-effort migration helpers
    if is_sqlite:
        _ensure_sqlite_schema()

    logger.info("init_db_completed")


def _ensure_sqlite_schema() -> None:
    """
    SQLite only:
    - Adds missing columns for message_logs (when upgrading schema without migrations)
    - Ensures unique index for webhook_events.external_event_id
    - Creates useful indexes for queries
    - Handles scheduled_messages table indexes
    """
    message_log_columns = {
        "conversation_event_id": "TEXT",
        "contact_id": "TEXT",
        "channel_id": "TEXT",
        "last_status": "TEXT",
        "last_status_at": "DATETIME",
        "direction": "TEXT",
        "message_id": "TEXT",
        "error_code": "INTEGER",
        "error_reason": "TEXT",
    }

    with engine.begin() as conn:
        # ── message_logs columns ──
        existing_columns = {
            row[1] for row in conn.execute(text("PRAGMA table_info(message_logs)")).fetchall()
        }

        logger.debug(
            "sqlite_existing_columns",
            extra={"extra": {"table": "message_logs", "columns": sorted(existing_columns)}},
        )

        for column, column_type in message_log_columns.items():
            if column not in existing_columns:
                conn.execute(text(f"ALTER TABLE message_logs ADD COLUMN {column} {column_type}"))
                logger.info(
                    "sqlite_column_added",
                    extra={"extra": {"table": "message_logs", "column": column, "type": column_type}},
                )

        # ── webhook_events unique index ──
        try:
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_webhook_external_event_id ON webhook_events (external_event_id)"
                )
            )
            logger.info("sqlite_unique_index_ensured", extra={"extra": {"index": "uq_webhook_external_event_id"}})
        except Exception as exc:
            logger.warning("sqlite_unique_index_create_failed", extra={"extra": {"error": str(exc)}})

        # ── message_logs indexes ──
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_message_logs_conversation_event_id ON message_logs (conversation_event_id)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_message_logs_contact_id ON message_logs (contact_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_message_logs_channel_id ON message_logs (channel_id)"))

        # ── scheduled_messages indexes ──
        try:
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_sched_res_tpl_to ON scheduled_messages (reservation_number, template_name, to_phone)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS "
                    "ix_sched_status_run_at ON scheduled_messages (status, run_at)"
                )
            )
            logger.info("sqlite_scheduled_messages_indexes_ensured")
        except Exception as exc:
            logger.warning("sqlite_scheduled_messages_index_failed", extra={"extra": {"error": str(exc)}})

    logger.info("sqlite_schema_migration_completed")
