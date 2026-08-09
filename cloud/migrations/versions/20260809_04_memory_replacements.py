"""Add user-scoped Memory replacement audit edges.

Revision ID: 20260809_04
Revises: 20260809_03
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_04"
down_revision = "20260809_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "memory_replacements",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("old_item_id", sa.String(length=32), nullable=False),
        sa.Column("old_memory_type", sa.String(length=32), nullable=False),
        sa.Column("old_summary", sa.Text(), nullable=False),
        sa.Column("old_source_ref", sa.Text(), nullable=True),
        sa.Column("old_happened_at", sa.Text(), nullable=True),
        sa.Column("old_extra_json", sa.JSON(), nullable=False),
        sa.Column("new_item_id", sa.String(length=32), nullable=False),
        sa.Column("new_memory_type", sa.String(length=32), nullable=False),
        sa.Column("new_summary", sa.Text(), nullable=False),
        sa.Column("new_source_ref", sa.Text(), nullable=True),
        sa.Column("new_happened_at", sa.Text(), nullable=True),
        sa.Column("new_extra_json", sa.JSON(), nullable=False),
        sa.Column("relation_type", sa.String(length=32), nullable=False),
        sa.Column("source_ref", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_memory_replacements_user_new",
        "memory_replacements",
        ["user_id", "new_item_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_memory_replacements_user_old",
        "memory_replacements",
        ["user_id", "old_item_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_memory_replacements_user_old", table_name="memory_replacements")
    op.drop_index("ix_memory_replacements_user_new", table_name="memory_replacements")
    op.drop_table("memory_replacements")
