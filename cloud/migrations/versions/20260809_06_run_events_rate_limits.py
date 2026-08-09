"""Add durable Run events and database-backed rate-limit counters.

Revision ID: 20260809_06
Revises: 20260809_05
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_06"
down_revision = "20260809_05"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "seq", name="uq_run_events_run_seq"),
    )
    op.create_index(
        "ix_run_events_run_created", "run_events", ["run_id", "created_at"]
    )
    op.create_index(
        "ix_run_events_user_created", "run_events", ["user_id", "created_at"]
    )
    op.create_table(
        "rate_limit_counters",
        sa.Column("subject_key", sa.String(length=256), nullable=False),
        sa.Column("scope", sa.String(length=64), nullable=False),
        sa.Column("window_start", sa.Integer(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("subject_key", "scope", "window_start"),
    )
    op.create_index(
        "ix_rate_limit_counters_expires_at",
        "rate_limit_counters",
        ["expires_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_rate_limit_counters_expires_at", table_name="rate_limit_counters"
    )
    op.drop_table("rate_limit_counters")
    op.drop_index("ix_run_events_user_created", table_name="run_events")
    op.drop_index("ix_run_events_run_created", table_name="run_events")
    op.drop_table("run_events")
