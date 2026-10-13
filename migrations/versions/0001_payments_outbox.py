"""Create payment state and transactional outbox.

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column(
            "metadata", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column("status", sa.String(9), server_default="pending", nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_event_id", sa.UUID(), nullable=False),
        sa.Column("current_attempt", sa.Integer(), server_default="1", nullable=False),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)),
        sa.Column("exhausted_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(500)),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"
        ),
        sa.CheckConstraint("current_attempt BETWEEN 1 AND 3", name="ck_payments_attempt"),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name="ck_payments_processed_status",
        ),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("payment_id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("routing_key", sa.String(32), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("publish_attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.String(500)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["payment_id"], ["payments.id"]),
        sa.UniqueConstraint(
            "payment_id", "event_type", "attempt_no", name="uq_outbox_payment_event_attempt"
        ),
        sa.CheckConstraint("attempt_no BETWEEN 1 AND 3", name="ck_outbox_attempt"),
        sa.CheckConstraint("publish_attempts >= 0", name="ck_outbox_publish_attempts"),
        sa.CheckConstraint(
            "event_type IN ('payment.process', 'payment.dead_letter')", name="ck_outbox_event_type"
        ),
    )
    op.create_index(
        "ix_outbox_pending_available",
        "outbox",
        ["available_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_pending_available", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
