from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
        CheckConstraint("current_attempt BETWEEN 1 AND 3", name="ck_payments_attempt"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name="ck_payments_processed_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(String(1000), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    status: Mapped[str] = mapped_column(
        String(9), nullable=False, default="pending", server_default="pending"
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, server_default=text("now()")
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_event_id: Mapped[UUID] = mapped_column(nullable=False, default=uuid4)
    current_attempt: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exhausted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(String(500))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint(
            "payment_id", "event_type", "attempt_no", name="uq_outbox_payment_event_attempt"
        ),
        CheckConstraint("attempt_no BETWEEN 1 AND 3", name="ck_outbox_attempt"),
        CheckConstraint("publish_attempts >= 0", name="ck_outbox_publish_attempts"),
        CheckConstraint(
            "event_type IN ('payment.process', 'payment.dead_letter')", name="ck_outbox_event_type"
        ),
        Index(
            "ix_outbox_pending_available",
            "available_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    routing_key: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, server_default=text("now()")
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_error: Mapped[str | None] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, server_default=text("now()")
    )
