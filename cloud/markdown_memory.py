"""Durable user-scoped profile documents for the original Markdown algorithms."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Iterator
from uuid import UUID

from sqlalchemy import Engine, delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from bus.events_lifecycle import TurnCommitted
from cloud.models import (
    MemoryProfileAppend,
    MemoryProfileBackup,
    MemoryProfileDocument,
    utc_now,
)
from core.memory.markdown import MarkdownMemoryMaintenance
from core.memory.store import DEFAULT_SELF_MD
from infra.providers.model_client_adapter import LLMProvider


_VALID_DOCUMENT_KINDS = frozenset(
    {"long_term", "self", "recent_context", "pending", "pending_snapshot"}
)


class UserScopedPostgresMarkdownStore:
    """PostgreSQL implementation of the profile store used by prompt/compaction."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._bound_user: ContextVar[UUID | None] = ContextVar(
            "cloud_markdown_memory_user", default=None
        )

    @contextmanager
    def bind_user(self, user_id: UUID | str) -> Iterator[None]:
        parsed = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        token = self._bound_user.set(parsed)
        try:
            yield
        finally:
            self._bound_user.reset(token)

    def close(self) -> None:
        self._engine.dispose()

    def read_long_term(self) -> str:
        return self._read("long_term")

    def write_long_term(self, content: str) -> None:
        self._write("long_term", content)

    def read_self(self) -> str:
        return self._read("self") or DEFAULT_SELF_MD

    def write_self(self, content: str) -> None:
        self._write("self", content)

    def read_recent_context(self) -> str:
        return self._read("recent_context")

    def write_recent_context(self, content: str) -> None:
        self._write("recent_context", content)

    def read_pending(self) -> str:
        return self._read("pending")

    def append_pending(self, facts: str) -> None:
        text = facts.strip()
        if text:
            self._append_document("pending", text)

    def append_pending_once(
        self,
        facts: str,
        *,
        source_ref: str,
        kind: str = "pending",
    ) -> bool:
        text = facts.strip()
        source = source_ref.strip()
        append_kind = kind.strip()
        if not text or not source or not append_kind:
            return False
        user_id = self._user_id()
        try:
            with Session(self._engine) as db, db.begin():
                existing = db.get(
                    MemoryProfileAppend,
                    {"user_id": user_id, "source_ref": source, "kind": append_kind},
                )
                if existing is not None:
                    return False
                document = self._locked_document(db, user_id, "pending")
                self._append_to_row(db, user_id, document, "pending", text)
                db.add(
                    MemoryProfileAppend(
                        user_id=user_id,
                        source_ref=source,
                        kind=append_kind,
                        payload=text,
                    )
                )
            return True
        except IntegrityError:
            return False

    def clear_pending(self) -> None:
        self._write("pending", "")

    def snapshot_pending(self) -> str:
        user_id = self._user_id()
        with Session(self._engine) as db, db.begin():
            pending = self._locked_document(db, user_id, "pending")
            snapshot = self._locked_document(db, user_id, "pending_snapshot")
            if snapshot is not None and snapshot.content:
                merged = snapshot.content.rstrip()
                if pending is not None and pending.content.strip():
                    merged += "\n" + pending.content
                if pending is None:
                    pending = MemoryProfileDocument(
                        user_id=user_id, kind="pending", content=merged
                    )
                    db.add(pending)
                else:
                    pending.content = merged
            if pending is None or not pending.content:
                return ""
            content = pending.content
            if snapshot is None:
                db.add(
                    MemoryProfileDocument(
                        user_id=user_id,
                        kind="pending_snapshot",
                        content=content,
                    )
                )
            else:
                snapshot.content = content
                snapshot.updated_at = utc_now()
            pending.content = ""
            pending.updated_at = utc_now()
            return content

    def commit_pending_snapshot(self) -> None:
        self._delete_document("pending_snapshot")

    def rollback_pending_snapshot(self) -> None:
        user_id = self._user_id()
        with Session(self._engine) as db, db.begin():
            snapshot = self._locked_document(db, user_id, "pending_snapshot")
            if snapshot is None:
                return
            pending = self._locked_document(db, user_id, "pending")
            merged = snapshot.content.rstrip()
            if pending is not None and pending.content.strip():
                merged += "\n" + pending.content
            if pending is None:
                db.add(
                    MemoryProfileDocument(
                        user_id=user_id, kind="pending", content=merged
                    )
                )
            else:
                pending.content = merged
                pending.updated_at = utc_now()
            db.delete(snapshot)

    def backup_long_term(self, backup_name: str = "MEMORY.bak.md") -> None:
        self._backup("long_term", backup_name)

    def backup_self(self, backup_name: str = "SELF.bak.md") -> None:
        self._backup("self", backup_name)

    def get_memory_context(self) -> str:
        long_term = self.read_long_term()
        return f"## Long-term Memory\n{long_term}" if long_term else ""

    def has_long_term_memory(self) -> bool:
        return bool(self.read_long_term().strip())

    def _user_id(self) -> UUID:
        user_id = self._bound_user.get()
        if user_id is None:
            raise RuntimeError("Cloud profile memory requires an active user binding")
        return user_id

    def _read(self, kind: str) -> str:
        self._validate_kind(kind)
        with Session(self._engine) as db:
            row = db.get(
                MemoryProfileDocument,
                {"user_id": self._user_id(), "kind": kind},
            )
            return row.content if row is not None else ""

    def _write(self, kind: str, content: str) -> None:
        self._validate_kind(kind)
        user_id = self._user_id()
        try:
            with Session(self._engine) as db, db.begin():
                row = self._locked_document(db, user_id, kind)
                if row is None:
                    db.add(
                        MemoryProfileDocument(
                            user_id=user_id, kind=kind, content=str(content)
                        )
                    )
                else:
                    row.content = str(content)
                    row.updated_at = utc_now()
        except IntegrityError:
            with Session(self._engine) as db, db.begin():
                row = self._locked_document(db, user_id, kind)
                if row is None:
                    raise
                row.content = str(content)
                row.updated_at = utc_now()

    def _append_document(self, kind: str, text: str) -> None:
        user_id = self._user_id()
        with Session(self._engine) as db, db.begin():
            row = self._locked_document(db, user_id, kind)
            self._append_to_row(db, user_id, row, kind, text)

    @staticmethod
    def _append_to_row(
        db: Session,
        user_id: UUID,
        row: MemoryProfileDocument | None,
        kind: str,
        text: str,
    ) -> None:
        if row is None:
            db.add(
                MemoryProfileDocument(
                    user_id=user_id, kind=kind, content=text.rstrip() + "\n"
                )
            )
        else:
            prefix = row.content.rstrip()
            row.content = (prefix + "\n" if prefix else "") + text.rstrip() + "\n"
            row.updated_at = utc_now()

    @staticmethod
    def _locked_document(
        db: Session, user_id: UUID, kind: str
    ) -> MemoryProfileDocument | None:
        return db.scalar(
            select(MemoryProfileDocument)
            .where(
                MemoryProfileDocument.user_id == user_id,
                MemoryProfileDocument.kind == kind,
            )
            .with_for_update()
        )

    def _delete_document(self, kind: str) -> None:
        with Session(self._engine) as db, db.begin():
            db.execute(
                delete(MemoryProfileDocument).where(
                    MemoryProfileDocument.user_id == self._user_id(),
                    MemoryProfileDocument.kind == kind,
                )
            )

    def _backup(self, kind: str, backup_name: str) -> None:
        content = self._read(kind)
        if not content:
            return
        with Session(self._engine) as db, db.begin():
            db.add(
                MemoryProfileBackup(
                    user_id=self._user_id(),
                    kind=kind,
                    backup_name=backup_name,
                    content=content,
                )
            )

    @staticmethod
    def _validate_kind(kind: str) -> None:
        if kind not in _VALID_DOCUMENT_KINDS:
            raise ValueError(f"unsupported profile document kind: {kind}")


class CloudMarkdownMemoryMaintenance(MarkdownMemoryMaintenance):
    """Bind the original maintenance queue to the event's durable user."""

    def __init__(self, *, store: UserScopedPostgresMarkdownStore, **kwargs: object) -> None:
        self._cloud_store = store
        self._session_users: dict[str, str] = {}
        super().__init__(store=store, **kwargs)  # type: ignore[arg-type]

    def on_turn_committed(self, event: TurnCommitted) -> None:
        user_id = str((event.extra or {}).get("principal_id") or "").strip()
        if not user_id:
            raise RuntimeError("Cloud consolidation requires principal_id")
        existing = self._session_users.get(event.session_key)
        if existing is not None and existing != user_id:
            raise RuntimeError("Cloud session cannot change memory owner")
        self._session_users[event.session_key] = user_id
        super().on_turn_committed(event)

    async def _run_maintenance_queue(self, session_key: str) -> None:
        user_id = self._session_users.get(session_key)
        if not user_id:
            raise RuntimeError("Cloud consolidation queue has no user owner")
        with self._cloud_store.bind_user(user_id):
            await super()._run_maintenance_queue(session_key)

    def _on_maintenance_done(
        self, task, session_key: str
    ) -> None:
        super()._on_maintenance_done(task, session_key)
        if session_key not in self._maintenance_tasks:
            self._session_users.pop(session_key, None)


@dataclass
class CloudMarkdownMemoryRuntime:
    store: UserScopedPostgresMarkdownStore
    maintenance: CloudMarkdownMemoryMaintenance
    descriptor: dict[str, object]


def build_cloud_markdown_memory_runtime(
    *,
    engine: Engine,
    provider: LLMProvider,
    model: str,
    keep_count: int,
    event_bus=None,
    recent_context_provider: LLMProvider | None = None,
    recent_context_model: str | None = None,
) -> CloudMarkdownMemoryRuntime:
    store = UserScopedPostgresMarkdownStore(engine)
    maintenance = CloudMarkdownMemoryMaintenance(
        store=store,
        provider=provider,
        model=model,
        keep_count=keep_count,
        event_bus=event_bus,
        recent_context_provider=recent_context_provider,
        recent_context_model=recent_context_model,
    )
    return CloudMarkdownMemoryRuntime(
        store=store,
        maintenance=maintenance,
        descriptor={"durable_backend": "postgresql", "tenant_scope": "user"},
    )
