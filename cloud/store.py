"""Transactional Cloud store for users, conversations, messages, and runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import time
from uuid import UUID, uuid4

from sqlalchemy import delete, exists, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from cloud.models import (
    AuthSession,
    AgentAutomation,
    Conversation,
    Message,
    RateLimitCounter,
    AutomationInboxEvent,
    Run,
    RunEvent,
    RunToolCheckpoint,
    User,
    WorkerInstance,
    DriftRunRecord,
    DriftSchedule,
    DriftJournal,
    DriftContinuum,
    ProactiveEventRecord,
    ProactivePendingAcknowledgement,
    ProactivePushState,
    ProactiveDelivery,
    ProactiveDecision,
    ProactiveSourceFeedback,
    ProactiveTick,
    ProactiveTickStep,
    utc_now,
)
from cloud.security import hash_password, hash_session_token, verify_password


class StoreConflictError(RuntimeError):
    pass


class StoreNotFoundError(RuntimeError):
    pass


class StoreStateError(RuntimeError):
    pass


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: int


class CloudStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def ping(self) -> None:
        async with self._sessions() as session:
            await session.execute(select(1))

    async def heartbeat_worker(
        self,
        worker_id: str,
        *,
        current_run_id: UUID | None = None,
        starting: bool = False,
    ) -> None:
        now = utc_now()
        async with self._sessions.begin() as session:
            worker = await session.scalar(
                select(WorkerInstance)
                .where(WorkerInstance.worker_id == worker_id[:200])
                .with_for_update()
            )
            if worker is None:
                session.add(
                    WorkerInstance(
                        worker_id=worker_id[:200],
                        status="running",
                        current_run_id=current_run_id,
                        started_at=now,
                        heartbeat_at=now,
                    )
                )
                return
            worker.status = "running"
            worker.current_run_id = current_run_id
            worker.heartbeat_at = now
            if starting:
                worker.started_at = now

    async def stop_worker(self, worker_id: str) -> None:
        async with self._sessions.begin() as session:
            worker = await session.scalar(
                select(WorkerInstance)
                .where(WorkerInstance.worker_id == worker_id[:200])
                .with_for_update()
            )
            if worker is not None:
                worker.status = "stopped"
                worker.current_run_id = None
                worker.heartbeat_at = utc_now()

    async def begin_tool_checkpoint(
        self,
        run_id: UUID,
        *,
        iteration: int,
        call_index: int,
        signature: str,
        tool_name: str,
        arguments: dict,
    ) -> tuple[str, RunToolCheckpoint]:
        """Fence one side-effect slot before invocation.

        Returns new, replay, ambiguous, or diverged.
        """
        async with self._sessions.begin() as session:
            run = await session.get(Run, run_id)
            if run is None:
                raise StoreNotFoundError("run not found")
            checkpoint = await session.scalar(
                select(RunToolCheckpoint)
                .where(
                    RunToolCheckpoint.run_id == run_id,
                    RunToolCheckpoint.iteration == max(0, iteration),
                    RunToolCheckpoint.call_index == max(0, call_index),
                )
                .with_for_update()
            )
            if checkpoint is None:
                checkpoint = RunToolCheckpoint(
                    run_id=run_id,
                    iteration=max(0, iteration),
                    call_index=max(0, call_index),
                    signature=signature,
                    tool_name=tool_name[:200],
                    arguments=dict(arguments),
                    status="started",
                )
                session.add(checkpoint)
                await session.flush()
                return "new", checkpoint
            if checkpoint.signature != signature:
                return "diverged", checkpoint
            if checkpoint.status == "started":
                return "ambiguous", checkpoint
            return "replay", checkpoint

    async def finish_tool_checkpoint(
        self,
        run_id: UUID,
        *,
        iteration: int,
        call_index: int,
        signature: str,
        status: str,
        output: str,
        mobile_attention: str | None = None,
    ) -> RunToolCheckpoint:
        if status not in {"success", "error"}:
            raise ValueError("tool checkpoint status must be success or error")
        async with self._sessions.begin() as session:
            checkpoint = await session.scalar(
                select(RunToolCheckpoint)
                .where(
                    RunToolCheckpoint.run_id == run_id,
                    RunToolCheckpoint.iteration == max(0, iteration),
                    RunToolCheckpoint.call_index == max(0, call_index),
                )
                .with_for_update()
            )
            if checkpoint is None or checkpoint.signature != signature:
                raise StoreStateError("tool checkpoint slot is missing or diverged")
            checkpoint.status = status
            checkpoint.output = output
            checkpoint.mobile_attention = mobile_attention
            checkpoint.completed_at = utc_now()
            return checkpoint

    @staticmethod
    async def _append_run_event(
        session: AsyncSession,
        run: Run,
        event_type: str,
        data: dict | None = None,
    ) -> RunEvent:
        """Append while the caller owns the Run row lock/transaction."""
        last_seq = await session.scalar(
            select(func.coalesce(func.max(RunEvent.seq), 0)).where(
                RunEvent.run_id == run.id
            )
        )
        event = RunEvent(
            run_id=run.id,
            user_id=run.user_id,
            seq=int(last_seq or 0) + 1,
            event_type=event_type,
            data=dict(data or {}),
        )
        session.add(event)
        return event

    async def register_user(self, email: str, password: str) -> User:
        normalized = email.strip().lower()
        if not normalized or "@" not in normalized:
            raise ValueError("a valid email is required")
        user = User(email=normalized, password_hash=hash_password(password))
        try:
            async with self._sessions.begin() as session:
                session.add(user)
        except IntegrityError as exc:
            raise StoreConflictError("email is already registered") from exc
        return user

    async def verify_user(self, email: str, password: str) -> User | None:
        async with self._sessions() as session:
            user = await session.scalar(
                select(User).where(User.email == email.strip().lower())
            )
        if user is None or not verify_password(user.password_hash, password):
            return None
        return user

    async def create_auth_session(
        self, user_id: UUID, raw_token: str, *, ttl_seconds: int
    ) -> AuthSession:
        auth = AuthSession(
            token_hash=hash_session_token(raw_token),
            user_id=user_id,
            expires_at=utc_now() + timedelta(seconds=max(60, ttl_seconds)),
        )
        async with self._sessions.begin() as session:
            session.add(auth)
        return auth

    async def user_for_token(self, raw_token: str) -> User | None:
        if not raw_token:
            return None
        now = utc_now()
        async with self._sessions() as session:
            return await session.scalar(
                select(User)
                .join(AuthSession, AuthSession.user_id == User.id)
                .where(
                    AuthSession.token_hash == hash_session_token(raw_token),
                    AuthSession.revoked_at.is_(None),
                    AuthSession.expires_at > now,
                )
            )

    async def revoke_auth_session(self, raw_token: str) -> None:
        if not raw_token:
            return
        async with self._sessions.begin() as session:
            auth = await session.scalar(
                select(AuthSession)
                .where(AuthSession.token_hash == hash_session_token(raw_token))
                .with_for_update()
            )
            if auth is not None and auth.revoked_at is None:
                auth.revoked_at = utc_now()

    async def create_conversation(
        self, user_id: UUID, title: str = "New conversation"
    ) -> Conversation:
        conversation = Conversation(user_id=user_id, title=title.strip()[:200] or "New conversation")
        async with self._sessions.begin() as session:
            session.add(conversation)
        return conversation

    async def list_conversations(
        self, user_id: UUID, *, limit: int = 50
    ) -> list[Conversation]:
        async with self._sessions() as session:
            result = await session.scalars(
                select(Conversation)
                .where(Conversation.user_id == user_id)
                .order_by(Conversation.updated_at.desc(), Conversation.id.desc())
                .limit(max(1, min(100, int(limit))))
            )
            return list(result)

    async def get_conversation(
        self, user_id: UUID, conversation_id: UUID
    ) -> Conversation:
        async with self._sessions() as session:
            conversation = await session.scalar(
                select(Conversation).where(
                    Conversation.id == conversation_id,
                    Conversation.user_id == user_id,
                )
            )
        if conversation is None:
            raise StoreNotFoundError("conversation not found")
        return conversation

    async def delete_conversation(
        self, user_id: UUID, conversation_id: UUID
    ) -> None:
        async with self._sessions.begin() as session:
            conversation = await session.scalar(
                select(Conversation)
                .where(
                    Conversation.id == conversation_id,
                    Conversation.user_id == user_id,
                )
                .with_for_update()
            )
            if conversation is None:
                raise StoreNotFoundError("conversation not found")
            running = await session.scalar(
                select(func.count(Run.id)).where(
                    Run.conversation_id == conversation_id,
                    Run.status == "running",
                )
            )
            if int(running or 0):
                raise StoreConflictError("conversation has a running Run")
            await session.delete(conversation)

    async def delete_user(self, user_id: UUID) -> None:
        async with self._sessions.begin() as session:
            user = await session.scalar(
                select(User).where(User.id == user_id).with_for_update()
            )
            if user is None:
                raise StoreNotFoundError("user not found")
            running = await session.scalar(
                select(func.count(Run.id)).where(
                    Run.user_id == user_id,
                    Run.status == "running",
                )
            )
            if int(running or 0):
                raise StoreConflictError("account has a running Run")
            await session.delete(user)

    async def get_automation(
        self, user_id: UUID, conversation_id: UUID
    ) -> AgentAutomation | None:
        await self.get_conversation(user_id, conversation_id)
        async with self._sessions() as session:
            return await session.scalar(
                select(AgentAutomation).where(
                    AgentAutomation.conversation_id == conversation_id,
                    AgentAutomation.user_id == user_id,
                )
            )

    async def configure_automation(
        self,
        user_id: UUID,
        conversation_id: UUID,
        *,
        proactive_enabled: bool,
        drift_enabled: bool,
        proactive_context: str = "",
    ) -> AgentAutomation:
        now = utc_now()
        async with self._sessions.begin() as session:
            conversation = await session.scalar(
                select(Conversation)
                .where(
                    Conversation.id == conversation_id,
                    Conversation.user_id == user_id,
                )
                .with_for_update()
            )
            if conversation is None:
                raise StoreNotFoundError("conversation not found")
            # Proactive/Drift state currently has user tenancy. Keep exactly one
            # active delivery target per user so reservoirs/journals can never be
            # consumed by a different conversation.
            if proactive_enabled or drift_enabled:
                others = list(
                    await session.scalars(
                        select(AgentAutomation)
                        .where(
                            AgentAutomation.user_id == user_id,
                            AgentAutomation.conversation_id != conversation_id,
                            AgentAutomation.enabled.is_(True),
                        )
                        .with_for_update()
                    )
                )
                for other in others:
                    other.enabled = False
                    other.proactive_enabled = False
                    other.drift_enabled = False
                    other.lease_owner = None
                    other.lease_expires_at = None
                    other.tick_token = None
                if others:
                    # These algorithms intentionally have user-scoped continuity.
                    # Switching their single active delivery target starts a new
                    # continuity epoch so unread items and skill journals can
                    # never surface in the newly selected conversation.
                    for model in (
                        ProactiveTickStep,
                        ProactiveTick,
                        ProactiveSourceFeedback,
                        ProactiveDecision,
                        ProactiveDelivery,
                        ProactivePushState,
                        ProactivePendingAcknowledgement,
                        ProactiveEventRecord,
                        DriftJournal,
                        DriftContinuum,
                        DriftSchedule,
                        DriftRunRecord,
                        AutomationInboxEvent,
                    ):
                        await session.execute(
                            delete(model).where(model.user_id == user_id)
                        )
            row = await session.get(AgentAutomation, conversation_id)
            if row is None:
                row = AgentAutomation(
                    conversation_id=conversation_id,
                    user_id=user_id,
                    next_tick_at=now,
                )
                session.add(row)
            row.proactive_enabled = bool(proactive_enabled)
            row.drift_enabled = bool(drift_enabled)
            row.enabled = row.proactive_enabled or row.drift_enabled
            row.proactive_context = str(proactive_context or "")[:20_000]
            row.next_tick_at = now
            row.lease_owner = None
            row.lease_expires_at = None
            row.tick_token = None
            row.last_error = ""
            row.updated_at = now
        return row

    async def ingest_proactive_event(
        self,
        user_id: UUID,
        conversation_id: UUID,
        *,
        kind: str,
        source_id: str,
        event_id: str,
        payload: dict,
    ) -> tuple[str, bool]:
        if kind not in {"alert", "content"}:
            raise ValueError("kind must be alert or content")
        source = source_id.strip()
        source_event = event_id.strip()
        if not source or not source_event:
            raise ValueError("source_id and event_id are required")
        # Include the active target in the stable identity. This makes a later
        # target switch unable to consume an older conversation's reservoir.
        digest = hashlib.sha256(
            f"{conversation_id}\0{source}\0{source_event}".encode("utf-8")
        ).hexdigest()
        item_id = f"cloud:{digest}"
        async with self._sessions.begin() as session:
            automation = await session.scalar(
                select(AgentAutomation).where(
                    AgentAutomation.conversation_id == conversation_id,
                    AgentAutomation.user_id == user_id,
                    AgentAutomation.enabled.is_(True),
                    AgentAutomation.proactive_enabled.is_(True),
                )
            )
            if automation is None:
                raise StoreStateError("proactive automation is not enabled")
            existing = await session.get(AutomationInboxEvent, item_id)
            if existing is not None:
                return item_id, False
            event = dict(payload or {})
            event.update(
                {
                    "kind": kind,
                    "item_id": item_id,
                    "_source": "",
                    "event_id": source_event,
                    "cloud_source_id": source,
                    "conversation_id": str(conversation_id),
                }
            )
            session.add(
                AutomationInboxEvent(
                    id=item_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    source_id=source,
                    source_event_id=source_event,
                    kind=kind,
                    payload=event,
                )
            )
            automation.next_tick_at = utc_now()
            return item_id, True

    async def fetch_automation_inbox(
        self, user_id: UUID, conversation_id: UUID
    ) -> list[dict]:
        async with self._sessions() as session:
            rows = list(
                await session.scalars(
                    select(AutomationInboxEvent)
                    .where(
                        AutomationInboxEvent.user_id == user_id,
                        AutomationInboxEvent.conversation_id == conversation_id,
                        AutomationInboxEvent.acknowledged_at.is_(None),
                    )
                    .order_by(AutomationInboxEvent.created_at, AutomationInboxEvent.id)
                )
            )
        return [
            {
                **dict(row.payload or {}),
                "kind": row.kind,
                "event_id": row.id,
                "item_id": row.id,
            }
            for row in rows
        ]

    async def acknowledge_automation_inbox(
        self,
        user_id: UUID,
        conversation_id: UUID,
        event_ids: list[str],
    ) -> None:
        ids = [str(item) for item in event_ids if str(item)]
        if not ids:
            return
        async with self._sessions.begin() as session:
            rows = list(
                await session.scalars(
                    select(AutomationInboxEvent)
                    .where(
                        AutomationInboxEvent.user_id == user_id,
                        AutomationInboxEvent.conversation_id == conversation_id,
                        AutomationInboxEvent.id.in_(ids),
                    )
                    .with_for_update()
                )
            )
            for row in rows:
                row.acknowledged_at = utc_now()

    async def claim_next_automation(
        self, worker_id: str, *, lease_seconds: int = 120
    ) -> AgentAutomation | None:
        now = utc_now()
        async with self._sessions.begin() as session:
            busy_run = exists().where(
                Run.conversation_id == AgentAutomation.conversation_id,
                Run.status.in_(("queued", "running")),
            )
            row = await session.scalar(
                select(AgentAutomation)
                .where(
                    AgentAutomation.enabled.is_(True),
                    AgentAutomation.next_tick_at <= now,
                    (
                        AgentAutomation.lease_expires_at.is_(None)
                        | (AgentAutomation.lease_expires_at <= now)
                    ),
                    ~busy_run,
                )
                .order_by(AgentAutomation.next_tick_at, AgentAutomation.conversation_id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if row is None:
                return None
            row.lease_owner = worker_id[:200]
            row.lease_expires_at = now + timedelta(seconds=max(10, lease_seconds))
            row.last_started_at = now
            row.tick_token = row.tick_token or uuid4().hex
            return row

    async def finish_automation(
        self,
        conversation_id: UUID,
        worker_id: str,
        *,
        next_tick_at: datetime,
        error: str = "",
    ) -> None:
        async with self._sessions.begin() as session:
            row = await session.scalar(
                select(AgentAutomation)
                .where(AgentAutomation.conversation_id == conversation_id)
                .with_for_update()
            )
            if row is None or row.lease_owner != worker_id[:200]:
                raise StoreStateError("automation lease is not owned by worker")
            row.next_tick_at = next_tick_at
            row.last_finished_at = utc_now()
            row.last_error = str(error or "")[:500]
            row.lease_owner = None
            row.lease_expires_at = None
            if not error:
                row.tick_token = None
            row.updated_at = utc_now()

    async def heartbeat_automation(
        self,
        conversation_id: UUID,
        worker_id: str,
        tick_token: str,
        *,
        lease_seconds: int,
    ) -> None:
        async with self._sessions.begin() as session:
            row = await session.scalar(
                select(AgentAutomation)
                .where(AgentAutomation.conversation_id == conversation_id)
                .with_for_update()
            )
            if (
                row is None
                or row.lease_owner != worker_id[:200]
                or row.tick_token != tick_token
            ):
                raise StoreStateError("automation lease was lost")
            row.lease_expires_at = utc_now() + timedelta(
                seconds=max(10, lease_seconds)
            )

    async def require_automation_tool_access(
        self,
        conversation_id: UUID,
        worker_id: str,
        tick_token: str,
    ) -> None:
        async with self._sessions() as session:
            owned = await session.scalar(
                select(func.count(AgentAutomation.conversation_id)).where(
                    AgentAutomation.conversation_id == conversation_id,
                    AgentAutomation.lease_owner == worker_id[:200],
                    AgentAutomation.tick_token == tick_token,
                    AgentAutomation.enabled.is_(True),
                )
            )
            busy = await session.scalar(
                select(func.count(Run.id)).where(
                    Run.conversation_id == conversation_id,
                    Run.status.in_(("queued", "running")),
                )
            )
        if not int(owned or 0) or int(busy or 0):
            raise StoreStateError(
                "automation tool invocation fenced by passive activity or lost lease"
            )

    async def load_automation_context(
        self, automation: AgentAutomation
    ) -> tuple[User, Conversation, list[Message]]:
        async with self._sessions() as session:
            user = await session.get(User, automation.user_id)
            conversation = await session.scalar(
                select(Conversation).where(
                    Conversation.id == automation.conversation_id,
                    Conversation.user_id == automation.user_id,
                )
            )
            messages = list(
                await session.scalars(
                    select(Message)
                    .where(Message.conversation_id == automation.conversation_id)
                    .order_by(Message.seq)
                )
            )
        if user is None or conversation is None:
            raise StoreStateError("automation references missing owner or conversation")
        return user, conversation, messages

    async def append_automation_message(
        self,
        *,
        user_id: UUID,
        conversation_id: UUID,
        worker_id: str,
        tick_token: str,
        source: str,
        content: str,
        metadata: dict | None = None,
    ) -> Message:
        delivery_key = f"{source}:{tick_token}"[:128]
        async with self._sessions.begin() as session:
            automation = await session.scalar(
                select(AgentAutomation)
                .where(
                    AgentAutomation.conversation_id == conversation_id,
                    AgentAutomation.user_id == user_id,
                )
                .with_for_update()
            )
            if (
                automation is None
                or automation.lease_owner != worker_id[:200]
                or automation.tick_token != tick_token
            ):
                raise StoreStateError("automation delivery has no matching lease")
            existing = await session.scalar(
                select(Message).where(
                    Message.conversation_id == conversation_id,
                    Message.delivery_key == delivery_key,
                )
            )
            if existing is not None:
                return existing
            busy = await session.scalar(
                select(func.count(Run.id)).where(
                    Run.conversation_id == conversation_id,
                    Run.status.in_(("queued", "running")),
                )
            )
            if int(busy or 0):
                raise StoreStateError("passive Run became active during automation tick")
            conversation = await session.scalar(
                select(Conversation)
                .where(
                    Conversation.id == conversation_id,
                    Conversation.user_id == user_id,
                )
                .with_for_update()
            )
            if conversation is None:
                raise StoreNotFoundError("conversation not found")
            message = Message(
                conversation_id=conversation_id,
                seq=conversation.next_message_seq,
                role="assistant",
                content=content,
                agent_metadata=dict(metadata or {}),
                delivery_key=delivery_key,
            )
            conversation.next_message_seq += 1
            conversation.updated_at = utc_now()
            session.add(message)
            await session.flush()
            return message

    async def list_messages(
        self,
        user_id: UUID,
        conversation_id: UUID,
        *,
        before_seq: int | None = None,
        limit: int = 100,
    ) -> list[Message]:
        await self.get_conversation(user_id, conversation_id)
        async with self._sessions() as session:
            query = select(Message).where(Message.conversation_id == conversation_id)
            if before_seq is not None:
                query = query.where(Message.seq < max(1, int(before_seq)))
            result = await session.scalars(
                query.order_by(Message.seq.desc()).limit(
                    max(1, min(500, int(limit)))
                )
            )
            return list(reversed(list(result)))

    async def append_user_message_and_run(
        self,
        user_id: UUID,
        conversation_id: UUID,
        content: str,
        *,
        idempotency_key: str | None = None,
    ) -> tuple[Message, Run]:
        if not content.strip():
            raise ValueError("message content must not be blank")
        key = (idempotency_key or "").strip() or None
        if key is not None and (len(key) > 128 or any(ord(c) < 33 for c in key)):
            raise ValueError("invalid Idempotency-Key")
        try:
            async with self._sessions.begin() as session:
                if key is not None:
                    existing = await session.scalar(
                        select(Run).where(
                            Run.user_id == user_id, Run.idempotency_key == key
                        )
                    )
                    if existing is not None:
                        if existing.conversation_id != conversation_id:
                            raise StoreConflictError(
                                "Idempotency-Key belongs to another conversation"
                            )
                        message = await session.get(Message, existing.input_message_id)
                        if message is None:
                            raise StoreStateError("idempotent Run input is missing")
                        return message, existing
                conversation = await session.scalar(
                    select(Conversation)
                    .where(
                        Conversation.id == conversation_id,
                        Conversation.user_id == user_id,
                    )
                    .with_for_update()
                )
                if conversation is None:
                    raise StoreNotFoundError("conversation not found")
                message = Message(
                    conversation_id=conversation.id,
                    seq=conversation.next_message_seq,
                    role="user",
                    content=content,
                )
                conversation.next_message_seq += 1
                conversation.updated_at = utc_now()
                automation = await session.get(AgentAutomation, conversation.id)
                if automation is not None and automation.lease_owner is not None:
                    # Passive input wins over background work. Preserve the tick
                    # token for idempotent retry, but revoke this worker now.
                    automation.lease_owner = None
                    automation.lease_expires_at = None
                    automation.next_tick_at = utc_now()
                session.add(message)
                await session.flush()
                run = Run(
                    user_id=user_id,
                    conversation_id=conversation.id,
                    input_message_id=message.id,
                    status="queued",
                    idempotency_key=key,
                )
                session.add(run)
                await session.flush()
                await self._append_run_event(
                    session,
                    run,
                    "run.queued",
                    {"status": "queued", "input_message_id": str(message.id)},
                )
            return message, run
        except IntegrityError:
            if key is None:
                raise
            async with self._sessions() as session:
                existing = await session.scalar(
                    select(Run).where(
                        Run.user_id == user_id, Run.idempotency_key == key
                    )
                )
                if existing is None:
                    raise
                if existing.conversation_id != conversation_id:
                    raise StoreConflictError(
                        "Idempotency-Key belongs to another conversation"
                    )
                message = await session.get(Message, existing.input_message_id)
                if message is None:
                    raise StoreStateError("idempotent Run input is missing")
                return message, existing

    async def get_run(self, user_id: UUID, run_id: UUID) -> Run:
        async with self._sessions() as session:
            run = await session.scalar(
                select(Run).where(Run.id == run_id, Run.user_id == user_id)
            )
        if run is None:
            raise StoreNotFoundError("run not found")
        return run

    async def claim_next_run(
        self, worker_id: str, *, lease_seconds: int = 60
    ) -> Run | None:
        running = aliased(Run)
        other_running = (
            exists(
                select(running.id).where(
                    running.conversation_id == Run.conversation_id,
                    running.status == "running",
                )
            )
            .correlate(Run)
        )
        try:
            async with self._sessions.begin() as session:
                run = await session.scalar(
                    select(Run)
                    .where(Run.status == "queued", ~other_running)
                    .order_by(Run.created_at, Run.id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if run is None:
                    return None
                now = utc_now()
                run.status = "running"
                run.lease_owner = worker_id
                run.lease_expires_at = now + timedelta(seconds=max(10, lease_seconds))
                run.heartbeat_at = now
                run.started_at = run.started_at or now
                run.attempt += 1
                await self._append_run_event(
                    session,
                    run,
                    "run.started",
                    {"status": "running", "attempt": run.attempt},
                )
        except IntegrityError:
            # Two consumers may observe separate queued rows for one conversation
            # before either commits. The partial unique index is the final arbiter;
            # the loser simply polls again instead of terminating its worker.
            return None
        return run

    async def load_run_input(
        self, run_id: UUID
    ) -> tuple[Run, User, Conversation, Message, list[Message]]:
        async with self._sessions() as session:
            run = await session.get(Run, run_id)
            if run is None:
                raise StoreNotFoundError("run not found")
            user = await session.get(User, run.user_id)
            conversation = await session.get(Conversation, run.conversation_id)
            input_message = await session.get(Message, run.input_message_id)
            history = list(
                await session.scalars(
                    select(Message)
                    .where(
                        Message.conversation_id == run.conversation_id,
                        Message.id != input_message.id,
                    )
                    .order_by(Message.seq)
                )
            ) if input_message is not None else []
        if user is None or conversation is None or input_message is None:
            raise StoreStateError("run references missing durable input")
        return run, user, conversation, input_message, history

    async def complete_run(
        self,
        run_id: UUID,
        worker_id: str,
        assistant_content: str,
        *,
        assistant_message_id: str | None = None,
        assistant_metadata: dict | None = None,
        conversation_metadata: dict | None = None,
        last_consolidated: int | None = None,
    ) -> tuple[Run, Message | None]:
        async with self._sessions.begin() as session:
            run = await session.scalar(
                select(Run).where(Run.id == run_id).with_for_update()
            )
            if run is None:
                raise StoreNotFoundError("run not found")
            if run.status != "running" or run.lease_owner != worker_id:
                raise StoreStateError("worker does not own a running run")
            if run.cancel_requested_at is not None:
                run.status = "cancelled"
                run.completed_at = utc_now()
                run.lease_owner = None
                run.lease_expires_at = None
                await self._append_run_event(
                    session, run, "run.cancelled", {"status": "cancelled"}
                )
                return run, None
            conversation = await session.scalar(
                select(Conversation)
                .where(Conversation.id == run.conversation_id)
                .with_for_update()
            )
            if conversation is None:
                raise StoreStateError("run conversation is missing")
            output = Message(
                **(
                    {"id": UUID(assistant_message_id)}
                    if assistant_message_id is not None
                    else {}
                ),
                conversation_id=conversation.id,
                seq=conversation.next_message_seq,
                role="assistant",
                content=assistant_content,
                agent_metadata=dict(assistant_metadata or {}),
            )
            conversation.next_message_seq += 1
            conversation.updated_at = utc_now()
            if conversation_metadata is not None:
                conversation.agent_metadata = dict(conversation_metadata)
            if last_consolidated is not None:
                conversation.last_consolidated = max(0, int(last_consolidated))
            session.add(output)
            await session.flush()
            run.output_message_id = output.id
            run.status = "completed"
            run.completed_at = utc_now()
            run.lease_owner = None
            run.lease_expires_at = None
            await self._append_run_event(
                session,
                run,
                "run.completed",
                {
                    "status": "completed",
                    "output_message_id": str(output.id),
                },
            )
        return run, output

    async def heartbeat_run(
        self, run_id: UUID, worker_id: str, *, lease_seconds: int = 60
    ) -> bool:
        """Renew an owned lease and report whether durable cancellation was requested."""
        async with self._sessions.begin() as session:
            run = await session.scalar(
                select(Run).where(Run.id == run_id).with_for_update()
            )
            if run is None:
                raise StoreNotFoundError("run not found")
            if run.status != "running" or run.lease_owner != worker_id:
                raise StoreStateError("worker does not own a running run")
            now = utc_now()
            run.heartbeat_at = now
            run.lease_expires_at = now + timedelta(seconds=max(10, lease_seconds))
            return run.cancel_requested_at is not None

    async def cancel_owned_run(self, run_id: UUID, worker_id: str) -> Run:
        """Move an owned running Run to its durable cancelled terminal state."""
        async with self._sessions.begin() as session:
            run = await session.scalar(
                select(Run).where(Run.id == run_id).with_for_update()
            )
            if run is None:
                raise StoreNotFoundError("run not found")
            if run.status != "running" or run.lease_owner != worker_id:
                raise StoreStateError("worker does not own a running run")
            run.status = "cancelled"
            run.completed_at = utc_now()
            run.lease_owner = None
            run.lease_expires_at = None
            await self._append_run_event(
                session, run, "run.cancelled", {"status": "cancelled"}
            )
        return run

    async def requeue_expired_runs(self, *, limit: int = 100) -> int:
        """Recover abandoned leases without racing healthy workers or other reapers."""
        now = utc_now()
        async with self._sessions.begin() as session:
            expired = list(
                await session.scalars(
                    select(Run)
                    .where(
                        Run.status == "running",
                        Run.lease_expires_at.is_not(None),
                        Run.lease_expires_at < now,
                    )
                    .order_by(Run.lease_expires_at, Run.id)
                    .limit(max(1, limit))
                    .with_for_update(skip_locked=True)
                )
            )
            for run in expired:
                run.lease_owner = None
                run.lease_expires_at = None
                if run.cancel_requested_at is not None:
                    run.status = "cancelled"
                    run.completed_at = now
                    await self._append_run_event(
                        session, run, "run.cancelled", {"status": "cancelled"}
                    )
                else:
                    run.status = "queued"
                    await self._append_run_event(
                        session,
                        run,
                        "run.requeued",
                        {"status": "queued", "attempt": run.attempt},
                    )
            return len(expired)

    async def fail_run(self, run_id: UUID, worker_id: str, error: str) -> Run:
        async with self._sessions.begin() as session:
            run = await session.scalar(
                select(Run).where(Run.id == run_id).with_for_update()
            )
            if run is None:
                raise StoreNotFoundError("run not found")
            if run.status != "running" or run.lease_owner != worker_id:
                raise StoreStateError("worker does not own a running run")
            if run.cancel_requested_at is not None:
                run.status = "cancelled"
                event_type = "run.cancelled"
            else:
                run.status = "failed"
                run.error = error[:8000]
                event_type = "run.failed"
            run.completed_at = utc_now()
            run.lease_owner = None
            run.lease_expires_at = None
            await self._append_run_event(
                session,
                run,
                event_type,
                {"status": run.status, **({"error": run.error} if run.error else {})},
            )
        return run

    async def request_cancel(self, user_id: UUID, run_id: UUID) -> Run:
        async with self._sessions.begin() as session:
            run = await session.scalar(
                select(Run)
                .where(Run.id == run_id, Run.user_id == user_id)
                .with_for_update()
            )
            if run is None:
                raise StoreNotFoundError("run not found")
            if run.status == "queued":
                run.status = "cancelled"
                run.completed_at = utc_now()
                await self._append_run_event(
                    session, run, "run.cancelled", {"status": "cancelled"}
                )
            elif run.status == "running":
                run.cancel_requested_at = utc_now()
                await self._append_run_event(
                    session,
                    run,
                    "run.cancel_requested",
                    {"status": "running"},
                )
        return run

    async def list_run_events(
        self,
        user_id: UUID,
        run_id: UUID,
        *,
        after_seq: int = 0,
        limit: int = 200,
    ) -> list[RunEvent]:
        """Read a tenant-scoped slice of the durable Run stream."""
        async with self._sessions() as session:
            owned = await session.scalar(
                select(Run.id).where(Run.id == run_id, Run.user_id == user_id)
            )
            if owned is None:
                raise StoreNotFoundError("run not found")
            events = await session.scalars(
                select(RunEvent)
                .where(
                    RunEvent.run_id == run_id,
                    RunEvent.user_id == user_id,
                    RunEvent.seq > max(0, after_seq),
                )
                .order_by(RunEvent.seq)
                .limit(min(max(1, limit), 1000))
            )
            return list(events)

    async def append_runtime_run_event(
        self, run_id: UUID, event_type: str, data: dict
    ) -> RunEvent:
        """Append an internal worker event under the Run row's ordering lock."""
        async with self._sessions.begin() as session:
            run = await session.scalar(
                select(Run).where(Run.id == run_id).with_for_update()
            )
            if run is None:
                raise StoreNotFoundError("run not found")
            event = await self._append_run_event(session, run, event_type, data)
            await session.flush()
            return event

    async def consume_rate_limit(
        self,
        subject_key: str,
        scope: str,
        *,
        limit: int,
        window_seconds: int,
    ) -> RateLimitDecision:
        """Atomically consume one fixed-window token using durable storage."""
        bounded_limit = max(1, int(limit))
        bounded_window = max(1, int(window_seconds))
        epoch = int(time.time())
        window_start = epoch - (epoch % bounded_window)
        retry_after = max(1, window_start + bounded_window - epoch)
        key = subject_key[:256]
        bucket_scope = scope[:64]
        for attempt in range(2):
            try:
                async with self._sessions.begin() as session:
                    counter = await session.scalar(
                        select(RateLimitCounter)
                        .where(
                            RateLimitCounter.subject_key == key,
                            RateLimitCounter.scope == bucket_scope,
                            RateLimitCounter.window_start == window_start,
                        )
                        .with_for_update()
                    )
                    if counter is None:
                        counter = RateLimitCounter(
                            subject_key=key,
                            scope=bucket_scope,
                            window_start=window_start,
                            count=1,
                            expires_at=utc_now() + timedelta(seconds=retry_after),
                        )
                        session.add(counter)
                        remaining = bounded_limit - 1
                    elif counter.count >= bounded_limit:
                        return RateLimitDecision(
                            False, bounded_limit, 0, retry_after
                        )
                    else:
                        counter.count += 1
                        remaining = bounded_limit - counter.count
                return RateLimitDecision(
                    True, bounded_limit, max(0, remaining), retry_after
                )
            except IntegrityError:
                if attempt:
                    raise
                # A concurrent first consumer inserted this bucket. Retry and
                # acquire its row lock instead of maintaining process-local state.
                continue
        raise AssertionError("rate-limit retry loop exhausted")
