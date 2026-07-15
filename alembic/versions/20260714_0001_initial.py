"""Create the complete phase-one truth-source schema."""

import sqlalchemy as sa
from alembic import op

revision = "20260714_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "documents",
        sa.Column("document_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_documents_tenant_id", "documents", ["tenant_id"])

    op.create_table(
        "document_versions",
        sa.Column("version_id", sa.String(36), primary_key=True),
        sa.Column(
            "document_id",
            sa.String(36),
            sa.ForeignKey("documents.document_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("object_key", sa.String(1000), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("mime_type", sa.String(255), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("tenant_id", "content_hash", name="uq_document_version_tenant_hash"),
        sa.UniqueConstraint("document_id", "version", name="uq_document_version_number"),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_document_versions_document_id", "document_versions", ["document_id"])
    op.create_index("ix_document_versions_tenant_id", "document_versions", ["tenant_id"])

    op.create_table(
        "ingestion_tasks",
        sa.Column("task_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column(
            "document_id",
            sa.String(36),
            sa.ForeignKey("documents.document_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "version_id",
            sa.String(36),
            sa.ForeignKey("document_versions.version_id", ondelete="CASCADE"),
        ),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(100)),
        sa.Column("error_message", sa.String(500)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    for column in ("tenant_id", "document_id", "version_id", "status"):
        op.create_index(f"ix_ingestion_tasks_{column}", "ingestion_tasks", [column])

    op.create_table(
        "chunks",
        sa.Column("chunk_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column(
            "document_id",
            sa.String(36),
            sa.ForeignKey("documents.document_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "version_id",
            sa.String(36),
            sa.ForeignKey("document_versions.version_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "parent_chunk_id",
            sa.String(36),
            sa.ForeignKey("chunks.chunk_id", ondelete="SET NULL"),
        ),
        sa.Column("chunk_kind", sa.String(16), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("title_path", sa.JSON(), nullable=False),
        sa.Column("source_location", sa.String(1000), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("embedding_model", sa.String(255), nullable=False),
        sa.Column("embedding_dimension", sa.Integer(), nullable=False),
        sa.Column("embedding_version", sa.String(100), nullable=False),
        sa.Column("vector_status", sa.String(32), nullable=False),
        sa.Column("graph_status", sa.String(32), nullable=False),
        sa.Column("vector_error", sa.String(255)),
        sa.Column("graph_error", sa.String(255)),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("version_id", "ordinal", name="uq_chunk_version_ordinal"),
        mysql_charset="utf8mb4",
    )
    for column in (
        "tenant_id",
        "document_id",
        "version_id",
        "parent_chunk_id",
        "content_hash",
    ):
        op.create_index(f"ix_chunks_{column}", "chunks", [column])
    op.create_index(
        "ix_chunks_tenant_status_valid", "chunks", ["tenant_id", "status", "valid_until"]
    )

    op.create_table(
        "chunk_acl",
        sa.Column("acl_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "chunk_id",
            sa.String(36),
            sa.ForeignKey("chunks.chunk_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("allowed_role", sa.String(128)),
        sa.Column("allowed_department", sa.String(128)),
        sa.Column("deny", sa.Boolean(), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
        mysql_charset="utf8mb4",
    )
    for column in ("chunk_id", "tenant_id", "allowed_role", "allowed_department"):
        op.create_index(f"ix_chunk_acl_{column}", "chunk_acl", [column])

    op.create_table(
        "outbox_events",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("event_version", sa.Integer(), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("aggregate_id", sa.String(36), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trace_id", sa.String(128), nullable=False),
        sa.Column("payload_summary", sa.JSON(), nullable=False),
        sa.Column("published", sa.Boolean(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        mysql_charset="utf8mb4",
    )
    for column in ("event_type", "tenant_id", "aggregate_id", "trace_id"):
        op.create_index(f"ix_outbox_events_{column}", "outbox_events", [column])

    op.create_table(
        "action_drafts",
        sa.Column("draft_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("user_id", sa.String(128), nullable=False),
        sa.Column("action_type", sa.String(50), nullable=False),
        sa.Column("idempotency_key", sa.String(200), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("risk_level", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("tenant_id", "idempotency_key", name="uq_draft_tenant_idempotency"),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_action_drafts_tenant_id", "action_drafts", ["tenant_id"])
    op.create_index("ix_action_drafts_user_id", "action_drafts", ["user_id"])

    op.create_table(
        "approval_requests",
        sa.Column("approval_id", sa.String(36), primary_key=True),
        sa.Column(
            "draft_id",
            sa.String(36),
            sa.ForeignKey("action_drafts.draft_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("decision_actor_id", sa.String(128)),
        sa.Column("idempotency_key", sa.String(200), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_approval_requests_draft_id", "approval_requests", ["draft_id"])
    op.create_index("ix_approval_requests_tenant_id", "approval_requests", ["tenant_id"])

    op.create_table(
        "sessions",
        sa.Column("session_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("user_id", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_sessions_tenant_id", "sessions", ["tenant_id"])
    op.create_index("ix_sessions_user_id", "sessions", ["user_id"])

    op.create_table(
        "messages",
        sa.Column("message_id", sa.String(36), primary_key=True),
        sa.Column(
            "session_id",
            sa.String(36),
            sa.ForeignKey("sessions.session_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_messages_session_id", "messages", ["session_id"])
    op.create_index("ix_messages_tenant_id", "messages", ["tenant_id"])

    op.create_table(
        "agent_runs",
        sa.Column("run_id", sa.String(36), primary_key=True),
        sa.Column("request_id", sa.String(128), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("model_version", sa.String(255), nullable=False),
        sa.Column("prompt_version", sa.String(100), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        mysql_charset="utf8mb4",
    )
    for column in ("request_id", "session_id", "tenant_id"):
        op.create_index(f"ix_agent_runs_{column}", "agent_runs", [column])

    op.create_table(
        "agent_steps",
        sa.Column("step_id", sa.String(36), primary_key=True),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("node_name", sa.String(100), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("input_summary", sa.JSON(), nullable=False),
        sa.Column("output_summary", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_agent_steps_run_id", "agent_steps", ["run_id"])
    op.create_index("ix_agent_steps_tenant_id", "agent_steps", ["tenant_id"])

    op.create_table(
        "tool_call_logs",
        sa.Column("tool_call_id", sa.String(36), primary_key=True),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("tool_name", sa.String(255), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("risk_level", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("input_summary", sa.JSON(), nullable=False),
        sa.Column("output_summary", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_tool_call_logs_run_id", "tool_call_logs", ["run_id"])
    op.create_index("ix_tool_call_logs_tenant_id", "tool_call_logs", ["tenant_id"])

    op.create_table(
        "faq_items",
        sa.Column("faq_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("question", sa.String(1000), nullable=False),
        sa.Column("answer", sa.Text(), nullable=False),
        sa.Column("patterns", sa.JSON(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_faq_items_tenant_id", "faq_items", ["tenant_id"])

    op.create_table(
        "audit_events",
        sa.Column("audit_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("request_id", sa.String(128)),
        sa.Column("fields", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_audit_events_tenant_id", "audit_events", ["tenant_id"])
    op.create_index("ix_audit_events_event_type", "audit_events", ["event_type"])
    op.create_index("ix_audit_events_request_id", "audit_events", ["request_id"])


def downgrade() -> None:
    for table in (
        "audit_events",
        "faq_items",
        "tool_call_logs",
        "agent_steps",
        "agent_runs",
        "messages",
        "sessions",
        "approval_requests",
        "action_drafts",
        "outbox_events",
        "chunk_acl",
        "chunks",
        "ingestion_tasks",
        "document_versions",
        "documents",
    ):
        op.drop_table(table)
