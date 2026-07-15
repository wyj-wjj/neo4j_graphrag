"""Add phase 1.5 session ordering and idempotent turn lifecycle."""

from __future__ import annotations

import hashlib

import sqlalchemy as sa
from alembic import op

revision = "20260714_0002"
down_revision = "20260714_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "sessions",
        sa.Column("revision", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "sessions",
        sa.Column("last_message_sequence", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("sessions", sa.Column("active_run_id", sa.String(36)))
    op.add_column(
        "sessions",
        sa.Column(
            "context_policy_version",
            sa.String(100),
            nullable=False,
            server_default="context-policy-v1",
        ),
    )
    op.create_index("ix_sessions_active_run_id", "sessions", ["active_run_id"])

    op.add_column("messages", sa.Column("client_turn_id", sa.String(128)))
    op.add_column("messages", sa.Column("run_id", sa.String(36)))
    op.add_column("messages", sa.Column("sequence", sa.Integer()))
    op.add_column(
        "messages",
        sa.Column("status", sa.String(32), nullable=False, server_default="committed"),
    )

    connection = op.get_bind()
    messages = sa.table(
        "messages",
        sa.column("message_id", sa.String(36)),
        sa.column("session_id", sa.String(36)),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("client_turn_id", sa.String(128)),
        sa.column("run_id", sa.String(36)),
        sa.column("sequence", sa.Integer()),
    )
    rows = connection.execute(
        sa.select(messages.c.message_id, messages.c.session_id).order_by(
            messages.c.session_id,
            messages.c.created_at,
            messages.c.message_id,
        )
    ).all()
    last_session: str | None = None
    sequence = 0
    last_sequences: dict[str, int] = {}
    for message_id, session_id in rows:
        if session_id != last_session:
            last_session = str(session_id)
            sequence = 0
        sequence += 1
        last_sequences[str(session_id)] = sequence
        connection.execute(
            messages.update()
            .where(messages.c.message_id == message_id)
            .values(
                client_turn_id=str(message_id),
                run_id=str(message_id),
                sequence=sequence,
            )
        )
    sessions = sa.table(
        "sessions",
        sa.column("session_id", sa.String(36)),
        sa.column("last_message_sequence", sa.Integer()),
    )
    for session_id, last_sequence in last_sequences.items():
        connection.execute(
            sessions.update()
            .where(sessions.c.session_id == session_id)
            .values(last_message_sequence=last_sequence)
        )

    with op.batch_alter_table("messages") as batch_op:
        batch_op.alter_column("client_turn_id", existing_type=sa.String(128), nullable=False)
        batch_op.alter_column("run_id", existing_type=sa.String(36), nullable=False)
        batch_op.alter_column("sequence", existing_type=sa.Integer(), nullable=False)
        batch_op.create_unique_constraint(
            "uq_message_session_sequence",
            ["tenant_id", "session_id", "sequence"],
        )
        batch_op.create_unique_constraint(
            "uq_message_turn_role",
            ["tenant_id", "session_id", "client_turn_id", "role"],
        )
    op.create_index("ix_messages_client_turn_id", "messages", ["client_turn_id"])
    op.create_index("ix_messages_run_id", "messages", ["run_id"])

    op.add_column("agent_runs", sa.Column("client_turn_id", sa.String(128)))
    op.add_column("agent_runs", sa.Column("input_hash", sa.String(64)))
    op.add_column("agent_runs", sa.Column("result_hash", sa.String(64)))
    op.add_column("agent_runs", sa.Column("result_payload", sa.JSON()))
    op.add_column("agent_runs", sa.Column("error_code", sa.String(100)))
    op.add_column(
        "agent_runs",
        sa.Column("state_version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "agent_runs",
        sa.Column(
            "checkpoint_version",
            sa.String(100),
            nullable=False,
            server_default="agent-state-v1",
        ),
    )
    op.add_column(
        "agent_runs",
        sa.Column(
            "context_policy_version",
            sa.String(100),
            nullable=False,
            server_default="context-policy-v1",
        ),
    )
    op.add_column(
        "agent_runs",
        sa.Column("recovery_source", sa.String(32), nullable=False, server_default="legacy"),
    )
    op.add_column(
        "agent_runs",
        sa.Column("session_revision", sa.Integer(), nullable=False, server_default="0"),
    )
    runs = sa.table(
        "agent_runs",
        sa.column("run_id", sa.String(36)),
        sa.column("client_turn_id", sa.String(128)),
        sa.column("input_hash", sa.String(64)),
        sa.column("status", sa.String(32)),
    )
    for (run_id,) in connection.execute(sa.select(runs.c.run_id)).all():
        stable_id = str(run_id)
        connection.execute(
            runs.update()
            .where(runs.c.run_id == run_id)
            .values(
                client_turn_id=stable_id,
                input_hash=hashlib.sha256(stable_id.encode()).hexdigest(),
                status="completed",
            )
        )
    with op.batch_alter_table("agent_runs") as batch_op:
        batch_op.alter_column("client_turn_id", existing_type=sa.String(128), nullable=False)
        batch_op.alter_column("input_hash", existing_type=sa.String(64), nullable=False)
        batch_op.create_unique_constraint(
            "uq_agent_run_client_turn",
            ["tenant_id", "session_id", "client_turn_id"],
        )
    op.create_index("ix_agent_runs_client_turn_id", "agent_runs", ["client_turn_id"])


def downgrade() -> None:
    op.drop_index("ix_agent_runs_client_turn_id", table_name="agent_runs")
    with op.batch_alter_table("agent_runs") as batch_op:
        batch_op.drop_constraint("uq_agent_run_client_turn", type_="unique")
        for column in (
            "session_revision",
            "recovery_source",
            "context_policy_version",
            "checkpoint_version",
            "state_version",
            "error_code",
            "result_payload",
            "result_hash",
            "input_hash",
            "client_turn_id",
        ):
            batch_op.drop_column(column)

    op.drop_index("ix_messages_run_id", table_name="messages")
    op.drop_index("ix_messages_client_turn_id", table_name="messages")
    with op.batch_alter_table("messages") as batch_op:
        batch_op.drop_constraint("uq_message_turn_role", type_="unique")
        batch_op.drop_constraint("uq_message_session_sequence", type_="unique")
        for column in ("status", "sequence", "run_id", "client_turn_id"):
            batch_op.drop_column(column)

    op.drop_index("ix_sessions_active_run_id", table_name="sessions")
    with op.batch_alter_table("sessions") as batch_op:
        for column in (
            "context_policy_version",
            "active_run_id",
            "last_message_sequence",
            "revision",
        ):
            batch_op.drop_column(column)
