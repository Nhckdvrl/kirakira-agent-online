"""Run-scoped transcript adapter that lets the existing pipeline replay Cloud state."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator

from session.manager import Session


@dataclass
class BoundTranscript:
    session: Session
    initial_message_count: int
    run_id: str | None = None
    saved: bool = False


class RunScopedTranscriptStore:
    """Expose one hydrated Cloud conversation through the synchronous transcript port.

    The agent pipeline remains unchanged and operates on its normal ``Session`` model.
    Binding is task-local, so separate workers cannot observe or mutate each other's
    conversation even when they share one pipeline construction.
    """

    def __init__(self) -> None:
        self._bound: ContextVar[BoundTranscript | None] = ContextVar(
            "cloud_bound_transcript", default=None
        )

    @contextmanager
    def bind(self, conversation_id: str, payload: dict[str, Any]) -> Iterator[BoundTranscript]:
        if self._bound.get() is not None:
            raise RuntimeError("a Cloud transcript is already bound in this context")
        messages = [self._hydrate_message(item) for item in payload.get("messages", [])]
        session = Session(
            key=conversation_id,
            messages=messages,
            created_at=str(payload.get("created_at") or ""),
            updated_at=str(payload.get("updated_at") or ""),
            metadata=dict(payload.get("metadata") or {}),
            last_consolidated=int(payload.get("last_consolidated") or 0),
        )
        binding = BoundTranscript(
            session=session,
            initial_message_count=len(messages),
            run_id=str(payload["run_id"]) if payload.get("run_id") else None,
        )
        token = self._bound.set(binding)
        try:
            yield binding
        finally:
            self._bound.reset(token)

    def get_or_create(self, key: str) -> Session:
        binding = self._require_binding(key)
        return binding.session

    def session_exists(self, key: str) -> bool:
        binding = self._bound.get()
        return binding is not None and binding.session.key == key

    def current_run_id(self) -> str | None:
        binding = self._bound.get()
        if binding is None:
            return None
        return binding.run_id

    def peek_next_message_id(self, session_key: str) -> str:
        session = self.get_or_create(session_key)
        if session._reserved_message_id is None:
            from uuid import uuid4

            session._reserved_message_id = uuid4().hex
        return session._reserved_message_id

    def save(self, session: Session) -> None:
        binding = self._require_binding(session.key)
        if binding.session is not session:
            raise RuntimeError("cannot save a Session outside its bound Cloud Run")
        binding.saved = True

    async def save_async(self, session: Session) -> None:
        self.save(session)

    def search_messages(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        binding = self._bound.get()
        if binding is None:
            raise RuntimeError("Cloud transcript search requires an active Run binding")
        needle = query.lower().strip()
        if not needle:
            return []
        matches = [
            message
            for message in reversed(binding.session.messages)
            if needle in str(message.get("content") or "").lower()
        ]
        return [
            {
                "source_ref": str(message.get("id") or ""),
                "session_key": binding.session.key,
                "role": str(message.get("role") or ""),
                "content": str(message.get("content") or "")[:500],
                "timestamp": str(message.get("timestamp") or ""),
            }
            for message in matches[: max(1, int(limit))]
        ]

    def fetch_messages(
        self, source_ref: str, context: int = 2
    ) -> list[dict[str, Any]]:
        binding = self._bound.get()
        if binding is None:
            raise RuntimeError("Cloud transcript fetch requires an active Run binding")
        messages = binding.session.messages
        target_index = next(
            (
                index
                for index, message in enumerate(messages)
                if str(message.get("id") or "") == source_ref
            ),
            None,
        )
        if target_index is None:
            return []
        radius = max(0, int(context))
        selected = messages[
            max(0, target_index - radius) : target_index + radius + 1
        ]
        return [
            {
                "source_ref": str(message.get("id") or ""),
                "role": str(message.get("role") or ""),
                "content": str(message.get("content") or ""),
                "timestamp": str(message.get("timestamp") or ""),
            }
            for message in selected
        ]

    def _require_binding(self, key: str) -> BoundTranscript:
        binding = self._bound.get()
        if binding is None:
            raise RuntimeError("Cloud transcript access requires an active Run binding")
        if binding.session.key != key:
            raise RuntimeError("bound Cloud transcript does not match conversation identity")
        return binding

    @staticmethod
    def _hydrate_message(item: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(item.get("metadata") or {})
        metadata.update(
            {
                "id": str(item["id"]),
                "seq": int(item["seq"]),
                "role": str(item["role"]),
                "content": str(item.get("content") or ""),
                "timestamp": str(item.get("created_at") or ""),
            }
        )
        return metadata
