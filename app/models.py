import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class WebhookEvent(Base):
    __tablename__ = "webhook_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    external_event_id: Mapped[str | None] = mapped_column(String(100), index=True)
    event_name: Mapped[str | None] = mapped_column(String(100))
    phone: Mapped[str | None] = mapped_column(String(32), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("external_event_id", name="uq_webhook_external_event_id"),
    )


class MessageLog(Base):
    __tablename__ = "message_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    phone: Mapped[str | None] = mapped_column(String(32), index=True)
    template_name: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20))
    provider_response: Mapped[str | None] = mapped_column(Text)
    conversation_event_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    contact_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    channel_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    last_status_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    direction: Mapped[str | None] = mapped_column(String(16), nullable=True)
    message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_message_logs_conversation_event_id", "conversation_event_id"),
        Index("ix_message_logs_contact_id", "contact_id"),
        Index("ix_message_logs_channel_id", "channel_id"),
    )


class SentNotification(Base):
    """Idempotency lock: at most ONE notification per (reservation, type, phone)."""
    __tablename__ = "sent_notifications"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    reservation_number: Mapped[str] = mapped_column(String(64))
    notification_type: Mapped[str] = mapped_column(String(64))
    phone: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("reservation_number", "notification_type", "phone", name="uq_sent_notif_res_type_phone"),
        Index("ix_sent_notif_res_num", "reservation_number"),
    )


class ScheduledMessage(Base):
    __tablename__ = "scheduled_messages"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # correlation
    external_event_id: Mapped[str | None] = mapped_column(String(100), index=True)
    reservation_number: Mapped[str | None] = mapped_column(String(64), index=True)

    # message
    to_phone: Mapped[str] = mapped_column(String(32), index=True)
    template_name: Mapped[str] = mapped_column(String(100))
    params_json: Mapped[str] = mapped_column(Text, default="[]")

    # scheduling
    run_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending/sent/failed/canceled
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(String(512), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        # يمنع تكرار نفس التذكير لنفس الحجز/العميل
        UniqueConstraint("reservation_number", "template_name", "to_phone", name="uq_sched_res_tpl_to"),
        Index("ix_sched_status_run_at", "status", "run_at"),
    )
