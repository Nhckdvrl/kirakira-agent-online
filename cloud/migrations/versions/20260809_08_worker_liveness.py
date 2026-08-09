"""Add durable worker liveness records.

Revision ID: 20260809_08
Revises: 20260809_07
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_08"
down_revision = "20260809_07"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "worker_instances",
        sa.Column("worker_id", sa.String(length=200), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("current_run_id", sa.Uuid(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('running', 'stopped')", name="ck_worker_instances_status"
        ),
        sa.ForeignKeyConstraint(["current_run_id"], ["runs.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("worker_id"),
    )
    op.create_index(
        "ix_worker_instances_heartbeat",
        "worker_instances",
        ["status", "heartbeat_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_worker_instances_heartbeat", table_name="worker_instances")
    op.drop_table("worker_instances")
