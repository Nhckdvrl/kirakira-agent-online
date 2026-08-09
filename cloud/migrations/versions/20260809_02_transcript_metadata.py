"""Persist the agent transcript state needed for lossless pipeline replay.

Revision ID: 20260809_02
Revises: 20260809_01
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_02"
down_revision = "20260809_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "conversations",
        sa.Column(
            "agent_metadata",
            sa.JSON(),
            server_default=sa.text("'{}'"),
            nullable=False,
        ),
    )
    op.add_column(
        "conversations",
        sa.Column(
            "last_consolidated",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "messages",
        sa.Column(
            "agent_metadata",
            sa.JSON(),
            server_default=sa.text("'{}'"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("messages", "agent_metadata")
    op.drop_column("conversations", "last_consolidated")
    op.drop_column("conversations", "agent_metadata")
