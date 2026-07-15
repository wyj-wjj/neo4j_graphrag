"""Persist structured conversation memory and redacted context manifests."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260714_0003"
down_revision = "20260714_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sessions", sa.Column("conversation_state", sa.JSON()))
    op.add_column(
        "sessions",
        sa.Column("summary", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column(
        "sessions",
        sa.Column("summary_version", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "sessions",
        sa.Column(
            "summary_through_sequence",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    sessions = sa.table(
        "sessions",
        sa.column("session_id", sa.String(36)),
        sa.column("tenant_id", sa.String(128)),
        sa.column("conversation_state", sa.JSON()),
    )
    connection = op.get_bind()
    for session_id, tenant_id in connection.execute(
        sa.select(sessions.c.session_id, sessions.c.tenant_id)
    ).all():
        connection.execute(
            sessions.update()
            .where(sessions.c.session_id == session_id)
            .values(
                conversation_state={
                    "state_version": 1,
                    "tenant_id": str(tenant_id),
                    "session_id": str(session_id),
                    "goal": None,
                    "task_stage": None,
                    "constraints": [],
                    "confirmed_facts": [],
                    "open_questions": [],
                    "summary": "",
                    "summary_version": 0,
                    "summary_through_sequence": 0,
                    "last_processed_sequence": 0,
                }
            )
        )

    op.create_table(
        "context_manifests",
        sa.Column("manifest_id", sa.String(36), primary_key=True),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("manifest_version", sa.Integer(), nullable=False),
        sa.Column("model", sa.String(255), nullable=False),
        sa.Column("context_policy_version", sa.String(100), nullable=False),
        sa.Column("model_context_window_tokens", sa.Integer(), nullable=False),
        sa.Column("reserved_output_tokens", sa.Integer(), nullable=False),
        sa.Column("input_budget_tokens", sa.Integer(), nullable=False),
        sa.Column("target_tokens", sa.JSON(), nullable=False),
        sa.Column("actual_tokens", sa.JSON(), nullable=False),
        sa.Column("actual_input_tokens", sa.Integer(), nullable=False),
        sa.Column("borrowed_tokens", sa.JSON(), nullable=False),
        sa.Column("selected_message_ids", sa.JSON(), nullable=False),
        sa.Column("selected_evidence_ids", sa.JSON(), nullable=False),
        sa.Column("selected_tool_result_ids", sa.JSON(), nullable=False),
        sa.Column("dropped_items", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("tenant_id", "run_id", name="uq_context_manifest_run"),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_context_manifests_run_id", "context_manifests", ["run_id"])
    op.create_index("ix_context_manifests_session_id", "context_manifests", ["session_id"])
    op.create_index("ix_context_manifests_tenant_id", "context_manifests", ["tenant_id"])


def downgrade() -> None:
    op.drop_table("context_manifests")
    with op.batch_alter_table("sessions") as batch_op:
        for column in (
            "summary_through_sequence",
            "summary_version",
            "summary",
            "conversation_state",
        ):
            batch_op.drop_column(column)
