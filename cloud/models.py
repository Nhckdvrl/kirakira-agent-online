"""SQLAlchemy models for the Cloud durable-state authority."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    email: Mapped[str] = mapped_column(String(320), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    conversations: Mapped[list[Conversation]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class AuthSession(Base):
    __tablename__ = "auth_sessions"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    title: Mapped[str] = mapped_column(String(200), nullable=False, default="New conversation")
    next_message_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    agent_metadata: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    last_consolidated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )

    user: Mapped[User] = relationship(back_populates="conversations")
    messages: Mapped[list[Message]] = relationship(
        back_populates="conversation", cascade="all, delete-orphan"
    )
    runs: Mapped[list[Run]] = relationship(
        back_populates="conversation", cascade="all, delete-orphan"
    )


class AgentAutomation(Base):
    """Durable scheduler state for one conversation's Proactive/Drift loop."""

    __tablename__ = "agent_automations"
    __table_args__ = (
        Index("ix_agent_automations_due", "enabled", "next_tick_at"),
        Index("ix_agent_automations_lease", "lease_expires_at"),
    )

    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    proactive_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    drift_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    proactive_context: Mapped[str] = mapped_column(Text, nullable=False, default="")
    next_tick_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    tick_token: Mapped[str | None] = mapped_column(String(64))
    last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class ScheduledJob(Base):
    """Tenant-scoped durable equivalent of the original JSON SchedulerService."""

    __tablename__ = "scheduled_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'cancelled', 'failed', 'missed')",
            name="ck_scheduled_jobs_status",
        ),
        CheckConstraint("tier IN ('instant', 'soft')", name="ck_scheduled_jobs_tier"),
        CheckConstraint(
            "trigger IN ('at', 'after', 'every')", name="ck_scheduled_jobs_trigger"
        ),
        Index("ix_scheduled_jobs_due", "status", "run_at"),
        Index("ix_scheduled_jobs_user_status", "user_id", "status", "run_at"),
        Index("ix_scheduled_jobs_lease", "lease_expires_at"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    remaining_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    tier: Mapped[str] = mapped_column(String(16), nullable=False, default="instant")
    trigger: Mapped[str] = mapped_column(String(16), nullable=False, default="at")
    cron_expr: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    timezone: Mapped[str] = mapped_column(String(100), nullable=False, default="UTC")
    name: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    fire_token: Mapped[str | None] = mapped_column(String(64))
    last_error: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class UserFile(Base):
    """Metadata authority for bytes stored in the isolated tenant workspace."""

    __tablename__ = "user_files"
    __table_args__ = (
        Index("ix_user_files_user_created", "user_id", "created_at"),
        Index("ix_user_files_conversation_created", "conversation_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    workspace_path: Mapped[str] = mapped_column(String(1000), nullable=False)
    filename: Mapped[str] = mapped_column(String(300), nullable=False)
    content_type: Mapped[str] = mapped_column(String(200), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class ChannelPairing(Base):
    __tablename__ = "channel_pairings"
    __table_args__ = (
        CheckConstraint(
            "provider IN ('telegram', 'qq', 'qqbot')", name="ck_channel_pairings_provider"
        ),
        Index("ix_channel_pairings_expiry", "expires_at"),
    )

    code_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class ChannelLink(Base):
    __tablename__ = "channel_links"
    __table_args__ = (
        UniqueConstraint(
            "provider", "external_chat_id", name="uq_channel_links_provider_chat"
        ),
        Index("ix_channel_links_conversation", "conversation_id", "enabled"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    external_user_id: Mapped[str] = mapped_column(String(300), nullable=False)
    external_chat_id: Mapped[str] = mapped_column(String(300), nullable=False)
    display_name: Mapped[str] = mapped_column(String(300), nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class ChannelInboundEvent(Base):
    __tablename__ = "channel_inbound_events"
    __table_args__ = (
        UniqueConstraint(
            "provider", "external_event_id", name="uq_channel_inbound_event"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    external_event_id: Mapped[str] = mapped_column(String(300), nullable=False)
    link_id: Mapped[UUID] = mapped_column(
        ForeignKey("channel_links.id", ondelete="CASCADE"), nullable=False
    )
    message_id: Mapped[UUID] = mapped_column(
        ForeignKey("messages.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class ChannelDelivery(Base):
    __tablename__ = "channel_deliveries"
    __table_args__ = (
        UniqueConstraint("link_id", "message_id", name="uq_channel_delivery_message"),
        CheckConstraint(
            "status IN ('pending', 'running', 'sent', 'failed')",
            name="ck_channel_deliveries_status",
        ),
        Index("ix_channel_deliveries_claim", "status", "created_at"),
        Index("ix_channel_deliveries_lease", "lease_expires_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    link_id: Mapped[UUID] = mapped_column(
        ForeignKey("channel_links.id", ondelete="CASCADE"), nullable=False
    )
    message_id: Mapped[UUID] = mapped_column(
        ForeignKey("messages.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CloudMcpServer(Base):
    """Per-user remote MCP declaration; credentials are envelope-encrypted."""

    __tablename__ = "cloud_mcp_servers"
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_cloud_mcp_server_user_name"),
        Index("ix_cloud_mcp_servers_user_enabled", "user_id", "enabled"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    base_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    encrypted_headers: Mapped[str] = mapped_column(Text, nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class CloudPlugin(Base):
    """Tenant-owned remote plugin service and its validated capability manifest."""

    __tablename__ = "cloud_plugins"
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_cloud_plugins_user_name"),
        Index("ix_cloud_plugins_user_enabled", "user_id", "enabled"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    base_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    encrypted_headers: Mapped[str] = mapped_column(Text, nullable=False, default="")
    manifest: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class CloudPluginTask(Base):
    """Durable timer for plugin jobs and Proactive source polling."""

    __tablename__ = "cloud_plugin_tasks"
    __table_args__ = (
        UniqueConstraint("plugin_id", "task_id", "kind", name="uq_cloud_plugin_task"),
        CheckConstraint("kind IN ('job', 'source')", name="ck_cloud_plugin_tasks_kind"),
        Index("ix_cloud_plugin_tasks_due", "enabled", "next_run_at"),
        Index("ix_cloud_plugin_tasks_lease", "lease_expires_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    plugin_id: Mapped[UUID] = mapped_column(
        ForeignKey("cloud_plugins.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[str] = mapped_column(String(200), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    next_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class CloudSubagentJob(Base):
    """Durable child-agent task owned by a user and parent conversation."""

    __tablename__ = "cloud_subagent_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed', 'cancelled')",
            name="ck_cloud_subagent_jobs_status",
        ),
        Index("ix_cloud_subagent_jobs_claim", "status", "created_at"),
        Index("ix_cloud_subagent_jobs_user_status", "user_id", "status"),
        Index("ix_cloud_subagent_jobs_lease", "lease_expires_at"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    task: Mapped[str] = mapped_column(Text, nullable=False)
    label: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    profile: Mapped[str] = mapped_column(String(32), nullable=False, default="research")
    max_iterations: Mapped[int] = mapped_column(Integer, nullable=False, default=8)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued")
    result: Mapped[str] = mapped_column(Text, nullable=False, default="")
    result_metadata: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CloudSkill(Base):
    __tablename__ = "cloud_skills"
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_cloud_skills_user_name"),
        Index("ix_cloud_skills_user_enabled", "user_id", "enabled"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(String(1000), nullable=False, default="-")
    when_to_use: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    body: Mapped[str] = mapped_column(Text, nullable=False)
    always: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class AutomationInboxEvent(Base):
    """External event waiting to be fetched by the canonical Proactive source stage."""

    __tablename__ = "automation_inbox_events"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "conversation_id",
            "source_id",
            "source_event_id",
            name="uq_automation_inbox_external_event",
        ),
        Index(
            "ix_automation_inbox_pending",
            "conversation_id",
            "acknowledged_at",
            "created_at",
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    source_event_id: Mapped[str] = mapped_column(String(300), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("conversation_id", "seq", name="uq_messages_conversation_seq"),
        UniqueConstraint(
            "conversation_id", "delivery_key", name="uq_messages_conversation_delivery"
        ),
        CheckConstraint("role IN ('user', 'assistant', 'system')", name="ck_messages_role"),
        Index("ix_messages_conversation_created", "conversation_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    agent_metadata: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    delivery_key: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    conversation: Mapped[Conversation] = relationship(back_populates="messages")


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'completed', 'failed', 'cancelled')",
            name="ck_runs_status",
        ),
        Index("ix_runs_status_created", "status", "created_at"),
        Index(
            "uq_runs_one_running_per_conversation",
            "conversation_id",
            unique=True,
            postgresql_where=text("status = 'running'"),
            sqlite_where=text("status = 'running'"),
        ),
        UniqueConstraint(
            "user_id", "idempotency_key", name="uq_runs_user_idempotency_key"
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False
    )
    input_message_id: Mapped[UUID] = mapped_column(
        ForeignKey("messages.id", ondelete="RESTRICT"), nullable=False, unique=True
    )
    output_message_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("messages.id", ondelete="SET NULL"), unique=True
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued")
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)
    idempotency_key: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    conversation: Mapped[Conversation] = relationship(back_populates="runs")
    input_message: Mapped[Message] = relationship(foreign_keys=[input_message_id])
    output_message: Mapped[Message | None] = relationship(foreign_keys=[output_message_id])


class RunEvent(Base):
    """Durable, ordered event stream for one Run."""

    __tablename__ = "run_events"
    __table_args__ = (
        UniqueConstraint("run_id", "seq", name="uq_run_events_run_seq"),
        Index("ix_run_events_run_created", "run_id", "created_at"),
        Index("ix_run_events_user_created", "user_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class RateLimitCounter(Base):
    """Durable fixed-window limiter used when no coordination cache is available."""

    __tablename__ = "rate_limit_counters"

    subject_key: Mapped[str] = mapped_column(String(256), primary_key=True)
    scope: Mapped[str] = mapped_column(String(64), primary_key=True)
    window_start: Mapped[int] = mapped_column(Integer, primary_key=True)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )


class WorkerInstance(Base):
    """Last-known worker liveness; leases remain the authority for Run ownership."""

    __tablename__ = "worker_instances"
    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'stopped')", name="ck_worker_instances_status"
        ),
        Index("ix_worker_instances_heartbeat", "status", "heartbeat_at"),
    )

    worker_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    current_run_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("runs.id", ondelete="SET NULL")
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class RunToolCheckpoint(Base):
    """Replay record and ambiguity fence for one ReAct tool invocation."""

    __tablename__ = "run_tool_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "run_id", "iteration", "call_index", name="uq_run_tool_checkpoint_slot"
        ),
        CheckConstraint(
            "status IN ('started', 'success', 'error')",
            name="ck_run_tool_checkpoints_status",
        ),
        Index("ix_run_tool_checkpoints_run", "run_id", "iteration", "call_index"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), nullable=False
    )
    iteration: Mapped[int] = mapped_column(Integer, nullable=False)
    call_index: Mapped[int] = mapped_column(Integer, nullable=False)
    signature: Mapped[str] = mapped_column(String(64), nullable=False)
    tool_name: Mapped[str] = mapped_column(String(200), nullable=False)
    arguments: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    output: Mapped[str | None] = mapped_column(Text)
    mobile_attention: Mapped[str | None] = mapped_column(String(32))
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DriftRunRecord(Base):
    __tablename__ = "drift_runs"
    __table_args__ = (
        Index("ix_drift_runs_user_skill_at", "user_id", "skill", "run_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    skill: Mapped[str] = mapped_column(String(200), nullable=False)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    briefing: Mapped[str] = mapped_column(Text, nullable=False, default="")
    message_result: Mapped[str] = mapped_column(
        String(32), nullable=False, default="silent"
    )


class DriftSchedule(Base):
    __tablename__ = "drift_schedules"

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    session_key: Mapped[str] = mapped_column(String(200), primary_key=True)
    timer_anchor: Mapped[str] = mapped_column(Text, nullable=False, default="")
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    sampled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class DriftJournal(Base):
    __tablename__ = "drift_journal"
    __table_args__ = (
        Index(
            "ix_drift_journal_user_skill_type_key",
            "user_id",
            "skill",
            "entry_type",
            "entry_key",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    skill: Mapped[str] = mapped_column(String(200), nullable=False)
    entry_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entry_key: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    run_id: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class DriftContinuum(Base):
    __tablename__ = "drift_continuum"

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    skill: Mapped[str] = mapped_column(String(200), primary_key=True)
    scratchpad: Mapped[str] = mapped_column(Text, nullable=False, default="")
    next_tendency: Mapped[str] = mapped_column(Text, nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ProactiveEventRecord(Base):
    __tablename__ = "proactive_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('unread', 'consumed', 'expired')",
            name="ck_proactive_events_status",
        ),
        Index(
            "ix_proactive_events_user_channel_status",
            "user_id",
            "channel",
            "status",
            "first_seen_at",
        ),
    )

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    item_id: Mapped[str] = mapped_column(String(300), primary_key=True)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    source_event_id: Mapped[str] = mapped_column(String(300), nullable=False, default="")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="unread")


class ProactivePendingAcknowledgement(Base):
    __tablename__ = "proactive_pending_acknowledgements"

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    source_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    source_event_id: Mapped[str] = mapped_column(String(300), primary_key=True)
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ProactivePushState(Base):
    __tablename__ = "proactive_push_state"

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    session_key: Mapped[str] = mapped_column(String(200), primary_key=True)
    last_push_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ProactiveDelivery(Base):
    __tablename__ = "proactive_deliveries"

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    session_key: Mapped[str] = mapped_column(String(200), primary_key=True)
    delivery_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ProactiveDecision(Base):
    __tablename__ = "proactive_decisions"
    __table_args__ = (
        Index("ix_proactive_decisions_user_at", "user_id", "decided_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    detail: Mapped[str] = mapped_column(Text, nullable=False, default="")


class ProactiveSourceFeedback(Base):
    __tablename__ = "proactive_source_feedback"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'sent')", name="ck_proactive_feedback_status"
        ),
    )

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    source_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    source_event_id: Mapped[str] = mapped_column(String(300), primary_key=True)
    feedback: Mapped[str] = mapped_column(String(100), primary_key=True)
    reason: Mapped[str] = mapped_column(String(300), nullable=False, default="")
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")


class ProactiveTick(Base):
    __tablename__ = "proactive_ticks"
    __table_args__ = (
        Index("ix_proactive_ticks_user_started", "user_id", "started_at"),
    )

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    tick_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_key: Mapped[str] = mapped_column(String(200), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    terminal: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    step_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str] = mapped_column(String(500), nullable=False, default="")


class ProactiveTickStep(Base):
    __tablename__ = "proactive_tick_steps"

    user_id: Mapped[UUID] = mapped_column(primary_key=True)
    tick_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    step_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    slot: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    terminal: Mapped[str | None] = mapped_column(String(100))
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    error: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    __table_args__ = (
        ForeignKeyConstraint(
            ["user_id", "tick_id"],
            ["proactive_ticks.user_id", "proactive_ticks.tick_id"],
            ondelete="CASCADE",
        ),
    )


class MemoryItem(Base):
    """A durable Memory v2 item owned by exactly one Cloud user."""

    __tablename__ = "memory_items"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "content_hash", "memory_type", name="uq_memory_items_user_hash_type"
        ),
        CheckConstraint(
            "status IN ('active', 'superseded')", name="ck_memory_items_status"
        ),
        Index("ix_memory_items_user_status_updated", "user_id", "status", "updated_at"),
        Index("ix_memory_items_user_type_status", "user_id", "memory_type", "status"),
        Index("ix_memory_items_user_source_ref", "user_id", "source_ref"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    memory_type: Mapped[str] = mapped_column(String(32), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(JSON)
    reinforcement: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    emotional_weight: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    extra_json: Mapped[dict | None] = mapped_column(JSON)
    source_ref: Mapped[str | None] = mapped_column(Text)
    happened_at: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class MemoryConsolidationEvent(Base):
    """Per-user idempotency record for consolidation writes."""

    __tablename__ = "memory_consolidation_events"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "source_ref", name="uq_memory_consolidation_user_source"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_ref: Mapped[str] = mapped_column(Text, nullable=False)
    item_id: Mapped[str | None] = mapped_column(
        ForeignKey("memory_items.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class MemoryReplacement(Base):
    """Snapshot edge used to restore superseded memories during undo."""

    __tablename__ = "memory_replacements"
    __table_args__ = (
        Index("ix_memory_replacements_user_old", "user_id", "old_item_id", "created_at"),
        Index("ix_memory_replacements_user_new", "user_id", "new_item_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    old_item_id: Mapped[str] = mapped_column(String(32), nullable=False)
    old_memory_type: Mapped[str] = mapped_column(String(32), nullable=False)
    old_summary: Mapped[str] = mapped_column(Text, nullable=False)
    old_source_ref: Mapped[str | None] = mapped_column(Text)
    old_happened_at: Mapped[str | None] = mapped_column(Text)
    old_extra_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    new_item_id: Mapped[str] = mapped_column(String(32), nullable=False)
    new_memory_type: Mapped[str] = mapped_column(String(32), nullable=False)
    new_summary: Mapped[str] = mapped_column(Text, nullable=False)
    new_source_ref: Mapped[str | None] = mapped_column(Text)
    new_happened_at: Mapped[str | None] = mapped_column(Text)
    new_extra_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    relation_type: Mapped[str] = mapped_column(
        String(32), nullable=False, default="supersede"
    )
    source_ref: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class MemoryProfileDocument(Base):
    __tablename__ = "memory_profile_documents"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('long_term', 'self', 'recent_context', 'pending', 'pending_snapshot')",
            name="ck_memory_profile_documents_kind",
        ),
    )

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    kind: Mapped[str] = mapped_column(String(32), primary_key=True)
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class MemoryProfileAppend(Base):
    __tablename__ = "memory_profile_appends"

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    source_ref: Mapped[str] = mapped_column(Text, primary_key=True)
    kind: Mapped[str] = mapped_column(String(64), primary_key=True)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class MemoryProfileBackup(Base):
    __tablename__ = "memory_profile_backups"
    __table_args__ = (
        Index("ix_memory_profile_backups_user_kind", "user_id", "kind", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    backup_name: Mapped[str] = mapped_column(String(200), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
