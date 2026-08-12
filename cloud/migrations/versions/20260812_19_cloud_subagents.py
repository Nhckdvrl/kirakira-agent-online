"""Add durable Cloud subagent jobs.

Revision ID: 20260812_19
Revises: 20260812_18
"""

from alembic import op
import sqlalchemy as sa


revision = "20260812_19"
down_revision = "20260812_18"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloud_subagent_jobs",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("task", sa.Text(), nullable=False),
        sa.Column("label", sa.String(200), nullable=False),
        sa.Column("profile", sa.String(32), nullable=False),
        sa.Column("max_iterations", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("result", sa.Text(), nullable=False),
        sa.Column("result_metadata", sa.JSON(), nullable=False),
        sa.Column("lease_owner", sa.String(200)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("status IN ('queued', 'running', 'completed', 'failed', 'cancelled')", name="ck_cloud_subagent_jobs_status"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_cloud_subagent_jobs_claim", "cloud_subagent_jobs", ["status", "created_at"])
    op.create_index("ix_cloud_subagent_jobs_user_status", "cloud_subagent_jobs", ["user_id", "status"])
    op.create_index("ix_cloud_subagent_jobs_lease", "cloud_subagent_jobs", ["lease_expires_at"])


def downgrade() -> None:
    op.drop_table("cloud_subagent_jobs")
