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
    content: str = Field(min_length=1, max_length=1_000_000)


class MessageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    conversation_id: UUID
    seq: int
    role: str
    content: str
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
