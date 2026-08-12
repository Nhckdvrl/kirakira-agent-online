"""Add multi-tenant channel identity and delivery tables.

Revision ID: 20260812_16
Revises: 20260812_15
"""

from alembic import op
import sqlalchemy as sa


revision = "20260812_16"
down_revision = "20260812_15"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "channel_pairings",
        sa.Column("code_hash", sa.String(64), primary_key=True),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "provider IN ('telegram', 'qq', 'qqbot')", name="ck_channel_pairings_provider"
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_channel_pairings_expiry", "channel_pairings", ["expires_at"])
    op.create_table(
        "channel_links",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("external_user_id", sa.String(300), nullable=False),
        sa.Column("external_chat_id", sa.String(300), nullable=False),
        sa.Column("display_name", sa.String(300), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("provider", "external_chat_id", name="uq_channel_links_provider_chat"),
    )
    op.create_index("ix_channel_links_conversation", "channel_links", ["conversation_id", "enabled"])
    op.create_table(
        "channel_inbound_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("external_event_id", sa.String(300), nullable=False),
        sa.Column("link_id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["link_id"], ["channel_links.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["message_id"], ["messages.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("provider", "external_event_id", name="uq_channel_inbound_event"),
    )
    op.create_table(
        "channel_deliveries",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("link_id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String(200), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(500), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('pending', 'running', 'sent', 'failed')", name="ck_channel_deliveries_status"),
        sa.ForeignKeyConstraint(["link_id"], ["channel_links.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["message_id"], ["messages.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("link_id", "message_id", name="uq_channel_delivery_message"),
    )
    op.create_index("ix_channel_deliveries_claim", "channel_deliveries", ["status", "created_at"])
    op.create_index("ix_channel_deliveries_lease", "channel_deliveries", ["lease_expires_at"])


def downgrade() -> None:
    op.drop_table("channel_deliveries")
    op.drop_table("channel_inbound_events")
    op.drop_table("channel_links")
    op.drop_table("channel_pairings")
