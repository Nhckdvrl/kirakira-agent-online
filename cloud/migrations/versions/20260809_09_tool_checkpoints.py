"""Add durable ReAct tool checkpoints.

Revision ID: 20260809_09
Revises: 20260809_08
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_09"
down_revision = "20260809_08"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run_tool_checkpoints",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("iteration", sa.Integer(), nullable=False),
        sa.Column("call_index", sa.Integer(), nullable=False),
        sa.Column("signature", sa.String(length=64), nullable=False),
        sa.Column("tool_name", sa.String(length=200), nullable=False),
        sa.Column("arguments", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("output", sa.Text(), nullable=True),
        sa.Column("mobile_attention", sa.String(length=32), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('started', 'success', 'error')",
            name="ck_run_tool_checkpoints_status",
        ),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "iteration", "call_index", name="uq_run_tool_checkpoint_slot"
        ),
    )
    op.create_index(
        "ix_run_tool_checkpoints_run",
        "run_tool_checkpoints",
        ["run_id", "iteration", "call_index"],
    )


def downgrade() -> None:
    op.drop_index("ix_run_tool_checkpoints_run", table_name="run_tool_checkpoints")
    op.drop_table("run_tool_checkpoints")
