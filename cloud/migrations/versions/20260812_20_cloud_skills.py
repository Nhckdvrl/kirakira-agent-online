"""Add tenant-scoped skills.

Revision ID: 20260812_20
Revises: 20260812_19
"""

from alembic import op
import sqlalchemy as sa


revision = "20260812_20"
down_revision = "20260812_19"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloud_skills",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column("when_to_use", sa.String(2000), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("always", sa.Boolean(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("user_id", "name", name="uq_cloud_skills_user_name"),
    )
    op.create_index("ix_cloud_skills_user_enabled", "cloud_skills", ["user_id", "enabled"])


def downgrade() -> None:
    op.drop_table("cloud_skills")
