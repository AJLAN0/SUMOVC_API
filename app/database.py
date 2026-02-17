import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import settings

logger = logging.getLogger("app.database")

DATABASE_URL = settings.DATABASE_URL

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, future=True, connect_args=connect_args)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)

Base = declarative_base()

logger.info(
    "database_engine_created",
    extra={"extra": {"url": DATABASE_URL, "backend": "sqlite" if DATABASE_URL.startswith("sqlite") else "other"}},
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
    if DATABASE_URL.startswith("sqlite"):
        _ensure_sqlite_schema()
    logger.info("init_db_completed")


def _ensure_sqlite_schema() -> None:
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
        existing_columns = {
            row[1] for row in conn.execute(text("PRAGMA table_info(message_logs)")).fetchall()
        }
        logger.debug(
            "sqlite_existing_columns",
            extra={"extra": {"table": "message_logs", "columns": sorted(existing_columns)}},
        )

        for column, column_type in message_log_columns.items():
            if column not in existing_columns:
                conn.execute(
                    text(f"ALTER TABLE message_logs ADD COLUMN {column} {column_type}")
                )
                logger.info(
                    "sqlite_column_added",
                    extra={"extra": {"table": "message_logs", "column": column, "type": column_type}},
                )

        try:
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_webhook_external_event_id ON webhook_events (external_event_id)"
                )
            )
            logger.info("sqlite_unique_index_ensured", extra={"extra": {"index": "uq_webhook_external_event_id"}})
        except Exception as exc:
            logger.warning(
                "sqlite_unique_index_create_failed",
                extra={"extra": {"error": str(exc)}},
            )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_message_logs_conversation_event_id ON message_logs (conversation_event_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_message_logs_contact_id ON message_logs (contact_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS "
                "ix_message_logs_channel_id ON message_logs (channel_id)"
            )
        )
    logger.info("sqlite_schema_migration_completed")
