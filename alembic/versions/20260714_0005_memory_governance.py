"""Add governed long-term memory and generation version manifests."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260714_0005"
down_revision = "20260714_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_memory_settings",
        sa.Column("tenant_id", sa.String(128), primary_key=True),
        sa.Column("user_id", sa.String(128), primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("auto_write_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        mysql_charset="utf8mb4",
    )
    op.create_table(
        "long_term_memories",
        sa.Column("memory_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("user_id", sa.String(128), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("source_turn_id", sa.String(128), nullable=False),
        sa.Column("confirmation_method", sa.String(32), nullable=False),
        sa.Column("confirmed_by", sa.String(128), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("sensitivity", sa.String(32), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("supersedes_memory_id", sa.String(36)),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "tenant_id", "user_id", "key", "version", name="uq_long_memory_key_version"
        ),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_long_term_memories_tenant_id", "long_term_memories", ["tenant_id"])
    op.create_index("ix_long_term_memories_user_id", "long_term_memories", ["user_id"])
    op.create_index(
        "ix_long_memory_active",
        "long_term_memories",
        ["tenant_id", "user_id", "status", "expires_at"],
    )
    op.create_table(
        "generation_manifests",
        sa.Column("manifest_id", sa.String(36), primary_key=True),
        sa.Column("run_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("manifest_version", sa.Integer(), nullable=False),
        sa.Column("prompt_bundle_version", sa.String(100), nullable=False),
        sa.Column("prompt_hashes", sa.JSON(), nullable=False),
        sa.Column("chat_model", sa.String(255), nullable=False),
        sa.Column("router_version", sa.String(100), nullable=False),
        sa.Column("embedding_model", sa.String(255), nullable=False),
        sa.Column("embedding_version", sa.String(100), nullable=False),
        sa.Column("rerank_model", sa.String(255), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("context_policy_version", sa.String(100), nullable=False),
        sa.Column("evaluation_set_version", sa.String(100), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("tenant_id", "run_id", name="uq_generation_manifest_run"),
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_generation_manifests_run_id", "generation_manifests", ["run_id"])
    op.create_index("ix_generation_manifests_tenant_id", "generation_manifests", ["tenant_id"])


def downgrade() -> None:
    op.drop_table("generation_manifests")
    op.drop_table("long_term_memories")
    op.drop_table("user_memory_settings")
