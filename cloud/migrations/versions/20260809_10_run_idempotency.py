"""Add per-user Run admission idempotency keys.

Revision ID: 20260809_10
Revises: 20260809_09
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_10"
down_revision = "20260809_09"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "runs", sa.Column("idempotency_key", sa.String(length=128), nullable=True)
    )
    op.create_unique_constraint(
        "uq_runs_user_idempotency_key", "runs", ["user_id", "idempotency_key"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_runs_user_idempotency_key", "runs", type_="unique")
    op.drop_column("runs", "idempotency_key")
