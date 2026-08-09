"""Add user-scoped durable Drift state.

Revision ID: 20260809_11
Revises: 20260809_10
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_11"
down_revision = "20260809_10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "drift_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("skill", sa.String(length=200), nullable=False),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("briefing", sa.Text(), nullable=False),
        sa.Column("message_result", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_drift_runs_user_skill_at", "drift_runs", ["user_id", "skill", "run_at"]
    )
    op.create_table(
        "drift_schedules",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("session_key", sa.String(length=200), nullable=False),
        sa.Column("timer_anchor", sa.Text(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sampled_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "session_key"),
    )
    op.create_table(
        "drift_journal",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("skill", sa.String(length=200), nullable=False),
        sa.Column("entry_type", sa.String(length=64), nullable=False),
        sa.Column("entry_key", sa.String(length=200), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_drift_journal_user_skill_type_key",
        "drift_journal",
        ["user_id", "skill", "entry_type", "entry_key"],
    )
    op.create_table(
        "drift_continuum",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("skill", sa.String(length=200), nullable=False),
        sa.Column("scratchpad", sa.Text(), nullable=False),
        sa.Column("next_tendency", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "skill"),
    )


def downgrade() -> None:
    op.drop_table("drift_continuum")
    op.drop_index(
        "ix_drift_journal_user_skill_type_key", table_name="drift_journal"
    )
    op.drop_table("drift_journal")
    op.drop_table("drift_schedules")
    op.drop_index("ix_drift_runs_user_skill_at", table_name="drift_runs")
    op.drop_table("drift_runs")
