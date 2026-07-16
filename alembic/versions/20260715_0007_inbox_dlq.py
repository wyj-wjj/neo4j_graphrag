"""Add consumer Inbox, DLQ, and repair audit tables."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "20260715_0007"
down_revision = "20260715_0006"
branch_labels = None
depends_on = None


def _large_text() -> sa.Text:
    return sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql")


def upgrade() -> None:
    op.create_table(
        "inbox_events",
        sa.Column("inbox_id", sa.String(36), primary_key=True),
        sa.Column("consumer_name", sa.String(128), nullable=False),
        sa.Column("event_id", sa.String(36), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("event_version", sa.Integer(), nullable=False),
        sa.Column("aggregate_version", sa.Integer(), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("aggregate_id", sa.String(36), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(128), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("consumer_name", "event_id", name="uq_inbox_consumer_event"),
    )
    for column in (
        "consumer_name",
        "event_id",
        "event_type",
        "tenant_id",
        "aggregate_id",
        "status",
        "lease_owner",
        "lease_expires_at",
        "processed_at",
    ):
        op.create_index(f"ix_inbox_events_{column}", "inbox_events", [column])

    op.create_table(
        "dead_letter_events",
        sa.Column("dlq_id", sa.String(36), primary_key=True),
        sa.Column("consumer_name", sa.String(128), nullable=False),
        sa.Column("event_id", sa.String(36), nullable=True),
        sa.Column("tenant_id", sa.String(128), nullable=True),
        sa.Column("event_type", sa.String(100), nullable=True),
        sa.Column("event_version", sa.Integer(), nullable=True),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("raw_payload_b64", _large_text(), nullable=False),
        sa.Column("failure_kind", sa.String(64), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("replay_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("consumer_name", "payload_hash", name="uq_dlq_consumer_payload"),
    )
    for column in (
        "consumer_name",
        "event_id",
        "tenant_id",
        "event_type",
        "failure_kind",
        "status",
    ):
        op.create_index(f"ix_dead_letter_events_{column}", "dead_letter_events", [column])

    op.create_table(
        "dlq_repairs",
        sa.Column("repair_id", sa.String(36), primary_key=True),
        sa.Column(
            "dlq_id",
            sa.String(36),
            sa.ForeignKey("dead_letter_events.dlq_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("original_payload_hash", sa.String(64), nullable=False),
        sa.Column("repaired_payload_hash", sa.String(64), nullable=False),
        sa.Column("repaired_payload_b64", _large_text(), nullable=False),
        sa.Column("operator_user_id", sa.String(128), nullable=False),
        sa.Column("reason", sa.String(500), nullable=False),
        sa.Column("target_event_version", sa.Integer(), nullable=False),
        sa.Column("replay_event_id", sa.String(36), nullable=False),
        sa.Column("replay_audit_id", sa.String(36), nullable=False, unique=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("dlq_id", "repaired_payload_hash", name="uq_dlq_repair_payload"),
    )
    for column in ("dlq_id", "operator_user_id", "replay_event_id", "status"):
        op.create_index(f"ix_dlq_repairs_{column}", "dlq_repairs", [column])


def downgrade() -> None:
    op.drop_table("dlq_repairs")
    op.drop_table("dead_letter_events")
    op.drop_table("inbox_events")
