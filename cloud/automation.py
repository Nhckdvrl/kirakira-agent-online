"""Durable Cloud scheduler for the unchanged Proactive and Drift algorithms."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack, suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import logging
from pathlib import Path
from typing import Any
from uuid import UUID

from agent.tools import build_default_registry
from agent.turns.result import OutboundDispatch, OutboundPort
from bus.queue import MessageBus
from cloud.drift_store import UserScopedPostgresDriftStore
from cloud.proactive_store import UserScopedPostgresProactiveStore
from cloud.store import CloudStore
from cloud.logging import safe_exception_summary
from cloud.transcript import RunScopedTranscriptStore
from plugins.drift_flow.runner import DriftRunner
from plugins.wake_proactive.sources import SourceRegistry
from proactive_v2.config import ProactiveConfig
from proactive_v2.loop import ProactiveLoop


logger = logging.getLogger("kirakira.cloud.automation")


class DatabaseAutomationOutboundPort(OutboundPort):
    """Treat an idempotent PostgreSQL message append as in-app delivery."""

    def __init__(
        self,
        store: CloudStore,
        *,
        user_id: UUID,
        conversation_id: UUID,
        worker_id: str,
        tick_token: str,
    ) -> None:
        self._store = store
        self._user_id = user_id
        self._conversation_id = conversation_id
        self._worker_id = worker_id
        self._tick_token = tick_token

    async def dispatch(self, outbound: OutboundDispatch) -> bool:
        metadata = dict(outbound.metadata or {})
        source = "drift" if metadata.get("drift") else "proactive"
        metadata.update({"proactive": True, "source": source})
        await self._store.append_automation_message(
            user_id=self._user_id,
            conversation_id=self._conversation_id,
            worker_id=self._worker_id,
            tick_token=self._tick_token,
            source=source,
            content=outbound.content,
            metadata=metadata,
        )
        return True


class DatabaseInboxSource:
    """Fetch/ACK port for API-ingested events; no host files or process globals."""

    id = "cloud-inbox"
    channels = ("alert", "content")

    def __init__(self, store: CloudStore, user_id: UUID, conversation_id: UUID) -> None:
        self._store = store
        self._user_id = user_id
        self._conversation_id = conversation_id

    async def fetch(self) -> list[dict[str, Any]]:
        return await self._store.fetch_automation_inbox(
            self._user_id, self._conversation_id
        )

    async def ack(
        self, event_ids: list[str], feedback: str | None = None
    ) -> None:
        del feedback
        await self._store.acknowledge_automation_inbox(
            self._user_id, self._conversation_id, list(event_ids)
        )


class CloudAutomationWorker:
    def __init__(
        self,
        *,
        store: CloudStore,
        sync_engine: Any,
        transcripts: RunScopedTranscriptStore,
        memory_services: Any,
        markdown_store: Any,
        model_client: Any,
        model: str,
        app_config: dict[str, Any],
        workspace: Path,
        execution_backend: Any,
        worker_id: str,
        poll_interval_seconds: float = 2.0,
        lease_seconds: int = 180,
    ) -> None:
        self._store = store
        self._sync_engine = sync_engine
        self._transcripts = transcripts
        self._memory_services = memory_services
        self._markdown_store = markdown_store
        self._model_client = model_client
        self._model = model
        self._app_config = app_config
        self._workspace = workspace
        self._execution_backend = execution_backend
        self.worker_id = worker_id[:200]
        self.poll_interval_seconds = max(0.1, poll_interval_seconds)
        self.lease_seconds = max(30, lease_seconds)
        self.heartbeat_interval_seconds = max(5.0, self.lease_seconds / 3)
        self._stop_requested = asyncio.Event()

    async def run_once(self) -> bool:
        automation = await self._store.claim_next_automation(
            self.worker_id, lease_seconds=self.lease_seconds
        )
        if automation is None:
            return False
        error = ""
        next_tick_at = datetime.now(UTC) + timedelta(minutes=5)
        heartbeat: asyncio.Task[None] | None = None
        try:
            heartbeat = asyncio.create_task(
                self._heartbeat(
                    automation.conversation_id, str(automation.tick_token or "")
                ),
                name=f"automation-heartbeat:{automation.conversation_id}",
            )
            user, conversation, messages = await self._store.load_automation_context(
                automation
            )
            payload = {
                "created_at": conversation.created_at.isoformat(),
                "updated_at": conversation.updated_at.isoformat(),
                "metadata": dict(conversation.agent_metadata or {}),
                "last_consolidated": conversation.last_consolidated,
                "messages": [
                    {
                        "id": str(message.id),
                        "role": message.role,
                        "content": message.content,
                        "seq": message.seq,
                        "created_at": message.created_at.isoformat(),
                        "metadata": dict(message.agent_metadata or {}),
                    }
                    for message in messages
                ],
            }
            with ExitStack() as stack:
                stack.enter_context(
                    self._transcripts.bind(str(conversation.id), payload)
                )
                stack.enter_context(
                    self._memory_services.engine.bind_user(str(user.id))
                )
                stack.enter_context(self._markdown_store.bind_user(user.id))
                next_tick_at = await self._execute_tick(automation)
        except Exception as exc:  # noqa: BLE001 - durable retry owns the failure
            error = safe_exception_summary(exc)
            next_tick_at = datetime.now(UTC) + timedelta(minutes=2)
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat
        # A passive message or administrator target switch may have revoked the
        # lease. In that case this stale worker must not overwrite durable truth.
        with suppress(Exception):
            await self._store.finish_automation(
                automation.conversation_id,
                self.worker_id,
                next_tick_at=next_tick_at,
                error=error,
            )
        return True

    async def _heartbeat(self, conversation_id: UUID, tick_token: str) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval_seconds)
            await self._store.heartbeat_automation(
                conversation_id,
                self.worker_id,
                tick_token,
                lease_seconds=self.lease_seconds,
            )

    async def _execute_tick(self, automation: Any) -> datetime:
        conversation_id = automation.conversation_id
        user_id = automation.user_id
        tick_token = str(automation.tick_token or "")
        if not tick_token:
            raise RuntimeError("claimed automation has no tick token")
        port = DatabaseAutomationOutboundPort(
            self._store,
            user_id=user_id,
            conversation_id=conversation_id,
            worker_id=self.worker_id,
            tick_token=tick_token,
        )
        bus = MessageBus()
        memory = self._markdown_store
        drift_state = UserScopedPostgresDriftStore(self._sync_engine, user_id)
        proactive_state = UserScopedPostgresProactiveStore(self._sync_engine, user_id)
        base = ProactiveConfig.from_app_config(
            self._app_config, default_model=self._model
        )
        cfg = replace(
            base,
            enabled=bool(automation.proactive_enabled),
            channel="cloud",
            chat_id=str(conversation_id),
            drift=replace(base.drift, enabled=bool(automation.drift_enabled)),
        )
        builtin_skills = Path(__file__).resolve().parents[1] / "plugins" / "drift_flow" / "builtin_skills"

        def registry_factory(_: str):
            registry = build_default_registry(
                self._workspace,
                memory=memory,
                session_manager=self._transcripts,
                memory_services=self._memory_services,
                execution_backend=self._execution_backend,
                workspace_backend=self._execution_backend,
            )

            async def guard(_call: Any, _tool: Any) -> None:
                await self._store.require_automation_tool_access(
                    conversation_id, self.worker_id, tick_token
                )

            registry.add_async_execution_guard(guard)
            return registry

        async def no_local_record(*_: Any) -> None:
            return None

        drift = DriftRunner(
            config=cfg.drift,
            workspace=self._workspace,
            bus=bus,
            session_manager=self._transcripts,  # type: ignore[arg-type]
            model_client=self._model_client,
            model=self._model,
            memory=memory,
            target_channel="cloud",
            target_chat_id=str(conversation_id),
            skill_roots_provider=lambda: (builtin_skills,),
            state_store=drift_state,
            tool_registry_factory=registry_factory,
            persist_transcripts=False,
            outbound_port=port,
            message_recorder=no_local_record,
            manage_example_skill=False,
        )
        now = datetime.now(UTC)
        if not automation.proactive_enabled:
            await drift.maybe_run(now, cfg.session_key)
            return now + timedelta(seconds=max(60, base.tick_interval_s0))
        sources = SourceRegistry()
        sources.add(DatabaseInboxSource(self._store, user_id, conversation_id))
        loop = ProactiveLoop(
            config=cfg,
            bus=bus,
            session_manager=self._transcripts,  # type: ignore[arg-type]
            model_client=self._model_client,
            sources=sources,
            state=proactive_state,
            memory=memory,
            memory_services=self._memory_services,
            drift_hook=drift.maybe_run,
            outbound_port=port,
            message_recorder=no_local_record,
            proactive_context_provider=lambda: automation.proactive_context,
            manage_context_file=False,
        )
        await loop.tick_once()
        return datetime.now(UTC) + timedelta(seconds=max(60, loop._next_interval()))

    async def run_forever(self) -> None:
        while not self._stop_requested.is_set():
            try:
                if await self.run_once():
                    continue
            except Exception:
                logger.exception("automation worker iteration failed")
            try:
                await asyncio.wait_for(
                    self._stop_requested.wait(), timeout=self.poll_interval_seconds
                )
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop_requested.set()
