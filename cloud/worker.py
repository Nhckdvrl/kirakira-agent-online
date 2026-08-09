"""Durable Run worker with leases, cancellation, polling, and graceful stop."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
import time
from typing import Protocol

from agent.turns.models import (
    AgentPrincipal,
    TurnMemoryScope,
    TurnOrigin,
    TurnRequest,
    TurnResult,
)
from cloud.executor import CLOUD_TRANSCRIPT_COMMIT_KEY, CLOUD_TRANSCRIPT_KEY
from cloud.store import CloudStore, StoreStateError
from cloud.observability import RUN_DURATION, RUNS, WORKER_ACTIVE_RUNS
from cloud.logging import safe_exception_summary


logger = logging.getLogger("kirakira.cloud.worker")


class AgentTurnExecutor(Protocol):
    async def execute(self, request: TurnRequest) -> TurnResult: ...


class CloudWorker:
    def __init__(
        self,
        store: CloudStore,
        executor: AgentTurnExecutor,
        *,
        worker_id: str,
        lease_seconds: int = 60,
        heartbeat_interval_seconds: float = 15,
        poll_interval_seconds: float = 1,
        reaper_interval_seconds: float = 30,
    ) -> None:
        self._store = store
        self._executor = executor
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.heartbeat_interval_seconds = max(0.01, heartbeat_interval_seconds)
        self.poll_interval_seconds = max(0.01, poll_interval_seconds)
        self.reaper_interval_seconds = max(0.01, reaper_interval_seconds)
        self._stop_requested = asyncio.Event()

    async def run_once(self) -> bool:
        run = await self._store.claim_next_run(
            self.worker_id, lease_seconds=self.lease_seconds
        )
        if run is None:
            return False
        await self._store.heartbeat_worker(
            self.worker_id, current_run_id=run.id
        )
        started_at = time.monotonic()
        WORKER_ACTIVE_RUNS.inc()
        execution: asyncio.Task[TurnResult] | None = None
        terminal_status = "failed"
        try:
            run, user, conversation, input_message, history = (
                await self._store.load_run_input(run.id)
            )
            request = TurnRequest(
                conversation_id=str(run.conversation_id),
                content=input_message.content,
                principal=AgentPrincipal(
                    str(user.id), kind="user", display_name=user.email
                ),
                origin=TurnOrigin("api", "cloud", str(run.id)),
                memory_scope=TurnMemoryScope("user", str(user.id)),
                metadata={
                    "run_id": str(run.id),
                    CLOUD_TRANSCRIPT_KEY: {
                        "created_at": conversation.created_at.isoformat(),
                        "updated_at": conversation.updated_at.isoformat(),
                        "metadata": dict(conversation.agent_metadata or {}),
                        "last_consolidated": conversation.last_consolidated,
                        "run_id": str(run.id),
                        "messages": [
                            {
                                "id": str(message.id),
                                "role": message.role,
                                "content": message.content,
                                "seq": message.seq,
                                "created_at": message.created_at.isoformat(),
                                "metadata": dict(message.agent_metadata or {}),
                            }
                            for message in history
                        ],
                    },
                },
            )
            execution = asyncio.create_task(self._executor.execute(request))
            while not execution.done():
                done, _ = await asyncio.wait(
                    {execution}, timeout=self.heartbeat_interval_seconds
                )
                if done:
                    break
                cancel_requested = await self._store.heartbeat_run(
                    run.id,
                    self.worker_id,
                    lease_seconds=self.lease_seconds,
                )
                if cancel_requested:
                    execution.cancel()
                    with suppress(asyncio.CancelledError):
                        await execution
                    await self._store.cancel_owned_run(run.id, self.worker_id)
                    terminal_status = "cancelled"
                    return True
            result = execution.result()
            transcript_commit = result.metadata.get(CLOUD_TRANSCRIPT_COMMIT_KEY)
            commit = transcript_commit if isinstance(transcript_commit, dict) else {}
            completed, _ = await self._store.complete_run(
                run.id,
                self.worker_id,
                result.content,
                assistant_message_id=commit.get("assistant_message_id"),
                assistant_metadata=commit.get("assistant_metadata"),
                conversation_metadata=commit.get("conversation_metadata"),
                last_consolidated=commit.get("last_consolidated"),
            )
            terminal_status = completed.status
        except asyncio.CancelledError:
            terminal_status = "abandoned"
            if execution is not None and not execution.done():
                execution.cancel()
                with suppress(asyncio.CancelledError):
                    await execution
            raise
        except Exception as exc:
            # Ownership can disappear only after lease recovery or another terminal
            # transition. In that case this worker must not overwrite durable truth.
            with suppress(StoreStateError):
                failed = await self._store.fail_run(
                    run.id, self.worker_id, safe_exception_summary(exc)
                )
                terminal_status = failed.status
            logger.exception(
                "run execution failed",
                extra={
                    "cloud_fields": {
                        "run_id": str(run.id),
                        "worker_id": self.worker_id,
                        "status": terminal_status,
                    }
                },
            )
        finally:
            WORKER_ACTIVE_RUNS.dec()
            if terminal_status in {"completed", "failed", "cancelled"}:
                RUNS.labels(terminal_status).inc()
            RUN_DURATION.labels(terminal_status).observe(
                max(0.0, time.monotonic() - started_at)
            )
            with suppress(Exception):
                await self._store.heartbeat_worker(self.worker_id)
        return True

    async def run_forever(self) -> None:
        """Poll durably until stop() is requested, finishing any active Run first."""
        loop = asyncio.get_running_loop()
        next_reap_at = 0.0
        next_worker_heartbeat_at = 0.0
        await self._store.heartbeat_worker(self.worker_id, starting=True)
        try:
            while not self._stop_requested.is_set():
                now = loop.time()
                if now >= next_reap_at:
                    await self._store.requeue_expired_runs()
                    next_reap_at = now + self.reaper_interval_seconds
                if now >= next_worker_heartbeat_at:
                    await self._store.heartbeat_worker(self.worker_id)
                    next_worker_heartbeat_at = now + min(
                        10.0, self.reaper_interval_seconds
                    )
                worked = await self.run_once()
                if worked:
                    continue
                try:
                    await asyncio.wait_for(
                        self._stop_requested.wait(), timeout=self.poll_interval_seconds
                    )
                except TimeoutError:
                    pass
        finally:
            await self._store.stop_worker(self.worker_id)

    def stop(self) -> None:
        """Request a graceful stop; a currently executing Run is allowed to finish."""
        self._stop_requested.set()
