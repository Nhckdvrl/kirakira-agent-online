"""Add user-scoped remote MCP declarations.

Revision ID: 20260812_17
Revises: 20260812_16
"""

from alembic import op
import sqlalchemy as sa


revision = "20260812_17"
down_revision = "20260812_16"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cloud_mcp_servers",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("base_url", sa.String(2000), nullable=False),
        sa.Column("encrypted_headers", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("user_id", "name", name="uq_cloud_mcp_server_user_name"),
    )
    op.create_index(
        "ix_cloud_mcp_servers_user_enabled",
        "cloud_mcp_servers",
        ["user_id", "enabled"],
    )


def downgrade() -> None:
    op.drop_index("ix_cloud_mcp_servers_user_enabled", table_name="cloud_mcp_servers")
    op.drop_table("cloud_mcp_servers")
