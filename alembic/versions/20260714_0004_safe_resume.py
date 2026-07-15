"""Add MySQL truth-source snapshots for safe interrupt recovery."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260714_0004"
down_revision = "20260714_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "safe_resume_snapshots",
        sa.Column("snapshot_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("user_id", sa.String(128), nullable=False),
        sa.Column("roles", sa.JSON(), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("request_id", sa.String(128), nullable=False),
        sa.Column("client_turn_id", sa.String(128), nullable=False),
        sa.Column("draft_id", sa.String(36), nullable=False),
        sa.Column("safe_node", sa.String(100), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("decision", sa.String(32)),
        sa.Column("next_action", sa.String(100), nullable=False),
        sa.Column("completed_side_effects", sa.JSON(), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("checkpoint_version", sa.String(100), nullable=False),
        sa.Column("recovery_source", sa.String(32), nullable=False),
        sa.Column("resume_result_hash", sa.String(64)),
        sa.Column("final_answer", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("tenant_id", "draft_id", name="uq_safe_resume_draft"),
        mysql_charset="utf8mb4",
    )
    for column in (
        "tenant_id",
        "session_id",
        "run_id",
        "draft_id",
        "status",
        "expires_at",
    ):
        op.create_index(
            f"ix_safe_resume_snapshots_{column}",
            "safe_resume_snapshots",
            [column],
        )


def downgrade() -> None:
    op.drop_table("safe_resume_snapshots")
