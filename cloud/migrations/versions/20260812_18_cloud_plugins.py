"""Add tenant remote plugins and durable jobs/sources.

Revision ID: 20260812_18
Revises: 20260812_17
"""

from alembic import op
import sqlalchemy as sa


revision = "20260812_18"
down_revision = "20260812_17"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloud_plugins",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("base_url", sa.String(2000), nullable=False),
        sa.Column("encrypted_headers", sa.Text(), nullable=False),
        sa.Column("manifest", sa.JSON(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("user_id", "name", name="uq_cloud_plugins_user_name"),
    )
    op.create_index("ix_cloud_plugins_user_enabled", "cloud_plugins", ["user_id", "enabled"])
    op.create_table(
        "cloud_plugin_tasks",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("plugin_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.String(200), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("interval_seconds", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(200)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(500), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("kind IN ('job', 'source')", name="ck_cloud_plugin_tasks_kind"),
        sa.ForeignKeyConstraint(["plugin_id"], ["cloud_plugins.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("plugin_id", "task_id", "kind", name="uq_cloud_plugin_task"),
    )
    op.create_index("ix_cloud_plugin_tasks_due", "cloud_plugin_tasks", ["enabled", "next_run_at"])
    op.create_index("ix_cloud_plugin_tasks_lease", "cloud_plugin_tasks", ["lease_expires_at"])


def downgrade() -> None:
    op.drop_table("cloud_plugin_tasks")
    op.drop_table("cloud_plugins")
