"""Add user-scoped durable Proactive state.

Revision ID: 20260809_12
Revises: 20260809_11
"""

from alembic import op
import sqlalchemy as sa


revision = "20260809_12"
down_revision = "20260809_11"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "proactive_events",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("item_id", sa.String(300), nullable=False),
        sa.Column("channel", sa.String(32), nullable=False),
        sa.Column("source_id", sa.String(200), nullable=False),
        sa.Column("source_event_id", sa.String(300), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.CheckConstraint(
            "status IN ('unread', 'consumed', 'expired')",
            name="ck_proactive_events_status",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "item_id"),
    )
    op.create_index(
        "ix_proactive_events_user_channel_status",
        "proactive_events",
        ["user_id", "channel", "status", "first_seen_at"],
    )
    op.create_table(
        "proactive_pending_acknowledgements",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.String(200), nullable=False),
        sa.Column("source_event_id", sa.String(300), nullable=False),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "source_id", "source_event_id"),
    )
    op.create_table(
        "proactive_push_state",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("session_key", sa.String(200), nullable=False),
        sa.Column("last_push_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "session_key"),
    )
    op.create_table(
        "proactive_deliveries",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("session_key", sa.String(200), nullable=False),
        sa.Column("delivery_key", sa.String(64), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "session_key", "delivery_key"),
    )
    op.create_table(
        "proactive_decisions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("action", sa.String(100), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_proactive_decisions_user_at",
        "proactive_decisions",
        ["user_id", "decided_at"],
    )
    op.create_table(
        "proactive_source_feedback",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.String(200), nullable=False),
        sa.Column("source_event_id", sa.String(300), nullable=False),
        sa.Column("feedback", sa.String(100), nullable=False),
        sa.Column("reason", sa.String(300), nullable=False),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'sent')", name="ck_proactive_feedback_status"
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "source_id", "source_event_id", "feedback"),
    )
    op.create_table(
        "proactive_ticks",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("tick_id", sa.String(64), nullable=False),
        sa.Column("session_key", sa.String(200), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("terminal", sa.String(100), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("step_count", sa.Integer(), nullable=False),
        sa.Column("error", sa.String(500), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "tick_id"),
    )
    op.create_index(
        "ix_proactive_ticks_user_started",
        "proactive_ticks",
        ["user_id", "started_at"],
    )
    op.create_table(
        "proactive_tick_steps",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("tick_id", sa.String(64), nullable=False),
        sa.Column("step_index", sa.Integer(), nullable=False),
        sa.Column("slot", sa.String(200), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("terminal", sa.String(100), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("error", sa.String(500), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id", "tick_id"],
            ["proactive_ticks.user_id", "proactive_ticks.tick_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("user_id", "tick_id", "step_index"),
    )


def downgrade() -> None:
    op.drop_table("proactive_tick_steps")
    op.drop_index("ix_proactive_ticks_user_started", table_name="proactive_ticks")
    op.drop_table("proactive_ticks")
    op.drop_table("proactive_source_feedback")
    op.drop_index("ix_proactive_decisions_user_at", table_name="proactive_decisions")
    op.drop_table("proactive_decisions")
    op.drop_table("proactive_deliveries")
    op.drop_table("proactive_push_state")
    op.drop_table("proactive_pending_acknowledgements")
    op.drop_index(
        "ix_proactive_events_user_channel_status", table_name="proactive_events"
    )
    op.drop_table("proactive_events")
