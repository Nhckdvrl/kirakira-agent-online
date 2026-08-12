"""Add isolated workspace file metadata.

Revision ID: 20260812_15
Revises: 20260812_14
"""

from alembic import op
import sqlalchemy as sa


revision = "20260812_15"
down_revision = "20260812_14"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_files",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("workspace_path", sa.String(1000), nullable=False),
        sa.Column("filename", sa.String(300), nullable=False),
        sa.Column("content_type", sa.String(200), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["conversation_id"], ["conversations.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_user_files_user_created", "user_files", ["user_id", "created_at"]
    )
    op.create_index(
        "ix_user_files_conversation_created",
        "user_files",
        ["conversation_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_user_files_conversation_created", table_name="user_files")
    op.drop_index("ix_user_files_user_created", table_name="user_files")
    op.drop_table("user_files")
