"""Add user-scoped durable Memory v2 storage.

Revision ID: 20260809_03
Revises: 20260809_02
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_03"
down_revision = "20260809_02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "memory_items",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("memory_type", sa.String(length=32), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("embedding", sa.JSON(), nullable=True),
        sa.Column("reinforcement", sa.Integer(), nullable=False),
        sa.Column("emotional_weight", sa.Integer(), nullable=False),
        sa.Column("extra_json", sa.JSON(), nullable=True),
        sa.Column("source_ref", sa.Text(), nullable=True),
        sa.Column("happened_at", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('active', 'superseded')", name="ck_memory_items_status"
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "content_hash",
            "memory_type",
            name="uq_memory_items_user_hash_type",
        ),
    )
    op.create_index(
        "ix_memory_items_user_source_ref",
        "memory_items",
        ["user_id", "source_ref"],
        unique=False,
    )
    op.create_index(
        "ix_memory_items_user_status_updated",
        "memory_items",
        ["user_id", "status", "updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_memory_items_user_type_status",
        "memory_items",
        ["user_id", "memory_type", "status"],
        unique=False,
    )
    op.create_table(
        "memory_consolidation_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("source_ref", sa.Text(), nullable=False),
        sa.Column("item_id", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["item_id"], ["memory_items.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id", "source_ref", name="uq_memory_consolidation_user_source"
        ),
    )
    op.create_index(
        op.f("ix_memory_consolidation_events_user_id"),
        "memory_consolidation_events",
        ["user_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_memory_consolidation_events_user_id"),
        table_name="memory_consolidation_events",
    )
    op.drop_table("memory_consolidation_events")
    op.drop_index("ix_memory_items_user_type_status", table_name="memory_items")
    op.drop_index("ix_memory_items_user_status_updated", table_name="memory_items")
    op.drop_index("ix_memory_items_user_source_ref", table_name="memory_items")
    op.drop_table("memory_items")
