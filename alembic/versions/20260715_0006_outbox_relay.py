"""Add reliable Outbox Relay lease and delivery state."""

import sqlalchemy as sa
from alembic import op

revision = "20260715_0006"
down_revision = "20260714_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "outbox_events",
        sa.Column("aggregate_version", sa.Integer(), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("lease_owner", sa.String(128), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("last_error_code", sa.String(128), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    connection = op.get_bind()
    connection.execute(
        sa.text("UPDATE outbox_events SET available_at = occurred_at WHERE available_at IS NULL")
    )
    connection.execute(
        sa.text("UPDATE outbox_events SET aggregate_version = 1 WHERE aggregate_version IS NULL")
    )
    with op.batch_alter_table("outbox_events") as batch:
        batch.alter_column(
            "available_at",
            existing_type=sa.DateTime(timezone=True),
            nullable=False,
        )
        batch.alter_column(
            "aggregate_version",
            existing_type=sa.Integer(),
            nullable=False,
        )
    op.create_index("ix_outbox_events_available_at", "outbox_events", ["available_at"])
    op.create_index("ix_outbox_events_lease_owner", "outbox_events", ["lease_owner"])
    op.create_index("ix_outbox_events_lease_expires_at", "outbox_events", ["lease_expires_at"])
    op.create_index("ix_outbox_events_published_at", "outbox_events", ["published_at"])


def downgrade() -> None:
    op.drop_index("ix_outbox_events_published_at", table_name="outbox_events")
    op.drop_index("ix_outbox_events_lease_expires_at", table_name="outbox_events")
    op.drop_index("ix_outbox_events_lease_owner", table_name="outbox_events")
    op.drop_index("ix_outbox_events_available_at", table_name="outbox_events")
    with op.batch_alter_table("outbox_events") as batch:
        batch.drop_column("published_at")
        batch.drop_column("last_error_code")
        batch.drop_column("lease_expires_at")
        batch.drop_column("lease_owner")
        batch.drop_column("available_at")
        batch.drop_column("aggregate_version")
