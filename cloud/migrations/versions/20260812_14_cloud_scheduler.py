"""Add tenant-scoped durable scheduled jobs.

Revision ID: 20260812_14
Revises: 20260809_13
"""

from alembic import op
import sqlalchemy as sa


revision = "20260812_14"
down_revision = "20260809_13"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scheduled_jobs",
        sa.Column("id", sa.String(64), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("interval_seconds", sa.Integer(), nullable=False),
        sa.Column("remaining_runs", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("tier", sa.String(16), nullable=False),
        sa.Column("trigger", sa.String(16), nullable=False),
        sa.Column("cron_expr", sa.String(200), nullable=False),
        sa.Column("timezone", sa.String(100), nullable=False),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("lease_owner", sa.String(200), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fire_token", sa.String(64), nullable=True),
        sa.Column("last_error", sa.String(500), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'cancelled', 'failed', 'missed')",
            name="ck_scheduled_jobs_status",
        ),
        sa.CheckConstraint("tier IN ('instant', 'soft')", name="ck_scheduled_jobs_tier"),
        sa.CheckConstraint(
            "trigger IN ('at', 'after', 'every')", name="ck_scheduled_jobs_trigger"
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["conversation_id"], ["conversations.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_scheduled_jobs_due", "scheduled_jobs", ["status", "run_at"]
    )
    op.create_index(
        "ix_scheduled_jobs_user_status",
        "scheduled_jobs",
        ["user_id", "status", "run_at"],
    )
    op.create_index(
        "ix_scheduled_jobs_lease", "scheduled_jobs", ["lease_expires_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_scheduled_jobs_lease", table_name="scheduled_jobs")
    op.drop_index("ix_scheduled_jobs_user_status", table_name="scheduled_jobs")
    op.drop_index("ix_scheduled_jobs_due", table_name="scheduled_jobs")
    op.drop_table("scheduled_jobs")

