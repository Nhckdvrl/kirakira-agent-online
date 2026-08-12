"""HTTP request and response schemas for Cloud V1."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from typing import Literal


class CredentialsIn(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=10, max_length=1024)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: str
    created_at: datetime


class ConversationCreate(BaseModel):
    title: str = Field(default="New conversation", max_length=200)


class ConversationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    title: str
    created_at: datetime
    updated_at: datetime


class MessageCreate(BaseModel):
    content: str = Field(default="", max_length=1_000_000)
    file_ids: list[UUID] = Field(default_factory=list, max_length=20)


class MessageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    conversation_id: UUID
    seq: int
    role: str
    content: str
    agent_metadata: dict
    created_at: datetime


class RunAccepted(BaseModel):
    message_id: UUID
    run_id: UUID


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    conversation_id: UUID
    input_message_id: UUID
    output_message_id: UUID | None
    status: str
    attempt: int
    cancel_requested_at: datetime | None
    error: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class RunEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    seq: int
    event_type: str
    data: dict
    created_at: datetime


class AutomationConfigIn(BaseModel):
    proactive_enabled: bool = False
    drift_enabled: bool = False
    proactive_context: str = Field(default="", max_length=20_000)


class AutomationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    conversation_id: UUID
    enabled: bool
    proactive_enabled: bool
    drift_enabled: bool
    proactive_context: str
    next_tick_at: datetime
    last_started_at: datetime | None
    last_finished_at: datetime | None
    last_error: str


class ProactiveEventIn(BaseModel):
    kind: Literal["alert", "content"]
    source_id: str = Field(min_length=1, max_length=200)
    event_id: str = Field(min_length=1, max_length=300)
    payload: dict = Field(default_factory=dict)


class ProactiveEventAccepted(BaseModel):
    item_id: str
    accepted: bool


class ScheduleCreate(BaseModel):
    message: str = Field(default="", max_length=1_000_000)
    run_at: str = Field(default="", max_length=100)
    delay_seconds: int = Field(default=0, ge=0)
    interval_seconds: int = Field(default=0, ge=0)
    repeat_count: int = Field(default=1, ge=1, le=1000)
    tier: Literal["instant", "soft"] = "instant"
    trigger: Literal["", "at", "after", "every"] = ""
    when: str = Field(default="", max_length=200)
    prompt: str = Field(default="", max_length=1_000_000)
    timezone: str = Field(default="UTC", max_length=100)
    name: str = Field(default="", max_length=200)
    request_time: str = Field(default="", max_length=100)


class ScheduleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    conversation_id: UUID
    message: str
    prompt: str
    run_at: datetime
    interval_seconds: int
    remaining_runs: int
    status: str
    tier: str
    trigger: str
    cron_expr: str
    timezone: str
    name: str
    last_error: str
    created_at: datetime


class FileUploadIn(BaseModel):
    filename: str = Field(min_length=1, max_length=300)
    content_type: str = Field(default="application/octet-stream", max_length=200)
    content_base64: str = Field(min_length=1, max_length=24_000_000)


class FileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    conversation_id: UUID
    filename: str
    content_type: str
    size_bytes: int
    sha256: str
    created_at: datetime


class ChannelPairingIn(BaseModel):
    provider: Literal["telegram", "qq", "qqbot"]


class ChannelPairingOut(BaseModel):
    provider: str
    code: str
    expires_at: datetime


class ChannelLinkOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    conversation_id: UUID
    provider: str
    external_user_id: str
    external_chat_id: str
    display_name: str
    enabled: bool
    created_at: datetime


class McpServerCreate(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
    base_url: str = Field(min_length=8, max_length=2000)
    headers: dict[str, str] = Field(default_factory=dict)


class McpServerOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    base_url: str
    enabled: bool
    created_at: datetime
    updated_at: datetime


class PluginCreate(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
    base_url: str = Field(min_length=8, max_length=2000)
    headers: dict[str, str] = Field(default_factory=dict)


class PluginOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    base_url: str
    manifest: dict
    enabled: bool
    created_at: datetime
    updated_at: datetime


class SubagentJobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    conversation_id: UUID
    task: str
    label: str
    profile: str
    max_iterations: int
    status: str
    result: str
    result_metadata: dict
    cancel_requested_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class SkillCreate(BaseModel):
    content: str = Field(min_length=1, max_length=1_000_000)


class SkillOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    description: str
    when_to_use: str
    always: bool
    enabled: bool
    created_at: datetime
    updated_at: datetime
