"""Add durable per-conversation Proactive/Drift scheduling.

Revision ID: 20260809_13
Revises: 20260809_12
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_13"
down_revision = "20260809_12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("messages", sa.Column("delivery_key", sa.String(128), nullable=True))
    op.create_unique_constraint(
        "uq_messages_conversation_delivery",
        "messages",
        ["conversation_id", "delivery_key"],
    )
    op.create_table(
        "agent_automations",
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("proactive_enabled", sa.Boolean(), nullable=False),
        sa.Column("drift_enabled", sa.Boolean(), nullable=False),
        sa.Column("proactive_context", sa.Text(), nullable=False),
        sa.Column("next_tick_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(200), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tick_token", sa.String(64), nullable=True),
        sa.Column("last_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(500), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("conversation_id"),
    )
    op.create_index("ix_agent_automations_user_id", "agent_automations", ["user_id"])
    op.create_index(
        "ix_agent_automations_due", "agent_automations", ["enabled", "next_tick_at"]
    )
    op.create_index(
        "ix_agent_automations_lease", "agent_automations", ["lease_expires_at"]
    )
    op.create_table(
        "automation_inbox_events",
        sa.Column("id", sa.String(64), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.String(200), nullable=False),
        sa.Column("source_event_id", sa.String(300), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["conversation_id"], ["conversations.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "conversation_id",
            "source_id",
            "source_event_id",
            name="uq_automation_inbox_external_event",
        ),
    )
    op.create_index(
        "ix_automation_inbox_pending",
        "automation_inbox_events",
        ["conversation_id", "acknowledged_at", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_automation_inbox_pending", table_name="automation_inbox_events"
    )
    op.drop_table("automation_inbox_events")
    op.drop_index("ix_agent_automations_lease", table_name="agent_automations")
    op.drop_index("ix_agent_automations_due", table_name="agent_automations")
    op.drop_index("ix_agent_automations_user_id", table_name="agent_automations")
    op.drop_table("agent_automations")
    op.drop_constraint(
        "uq_messages_conversation_delivery", "messages", type_="unique"
    )
    op.drop_column("messages", "delivery_key")
