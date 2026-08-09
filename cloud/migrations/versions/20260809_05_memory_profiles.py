"""Add durable user-scoped Markdown/profile memory state.

Revision ID: 20260809_05
Revises: 20260809_04
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_05"
down_revision = "20260809_04"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "memory_profile_documents",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "kind IN ('long_term', 'self', 'recent_context', 'pending', 'pending_snapshot')",
            name="ck_memory_profile_documents_kind",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "kind"),
    )
    op.create_table(
        "memory_profile_appends",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("source_ref", sa.Text(), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "source_ref", "kind"),
    )
    op.create_table(
        "memory_profile_backups",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("backup_name", sa.String(length=200), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_memory_profile_backups_user_kind",
        "memory_profile_backups",
        ["user_id", "kind", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_memory_profile_backups_user_kind", table_name="memory_profile_backups"
    )
    op.drop_table("memory_profile_backups")
    op.drop_table("memory_profile_appends")
    op.drop_table("memory_profile_documents")
