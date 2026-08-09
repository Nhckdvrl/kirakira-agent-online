"""Bridge core streaming/tool lifecycle events into the durable Run stream."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import UUID

from bus.event_bus import EventSubscription
from bus.events_lifecycle import StreamDeltaReady, ToolCallCompleted, ToolCallStarted
from cloud.store import CloudStore


@dataclass
class _DeltaBuffer:
    iteration: int
    content: str = ""


class DurableRunStreamBridge:
    def __init__(
        self,
        store: CloudStore,
        *,
        flush_interval_seconds: float = 0.075,
        flush_chars: int = 512,
    ) -> None:
        self._store = store
        self._flush_interval = max(0.01, flush_interval_seconds)
        self._flush_chars = max(32, flush_chars)
        self._buffers: dict[UUID, _DeltaBuffer] = {}
        self._timers: dict[UUID, asyncio.Task[None]] = {}
        self._locks: dict[UUID, asyncio.Lock] = {}
        self._subscriptions: list[EventSubscription] = []

    def bind(self, event_bus) -> None:
        self._subscriptions.extend(
            [
                event_bus.on(StreamDeltaReady, self.on_delta),
                event_bus.on(ToolCallStarted, self.on_tool_started),
                event_bus.on(ToolCallCompleted, self.on_tool_completed),
            ]
        )

    async def on_delta(self, event: StreamDeltaReady) -> StreamDeltaReady:
        if not event.content_delta:
            return event
        run_id = self._run_id(event.chat_id)
        if run_id is None:
            return event
        flush_now = False
        async with self._lock(run_id):
            buffer = self._buffers.get(run_id)
            if buffer is not None and buffer.iteration != event.iteration:
                flush_now = True
            if flush_now:
                await self._flush_locked(run_id)
                buffer = None
            if buffer is None:
                buffer = _DeltaBuffer(iteration=event.iteration)
                self._buffers[run_id] = buffer
            buffer.content += event.content_delta
            if len(buffer.content) >= self._flush_chars:
                await self._flush_locked(run_id)
            elif run_id not in self._timers:
                self._timers[run_id] = asyncio.create_task(
                    self._delayed_flush(run_id), name=f"run-stream:{run_id}"
                )
        return event

    async def on_tool_started(self, event: ToolCallStarted) -> ToolCallStarted:
        run_id = self._run_id(event.chat_id)
        if run_id is not None:
            await self.flush_run(run_id)
            await self._store.append_runtime_run_event(
                run_id,
                "tool.started",
                {"tool": event.tool_name, "iteration": event.iteration},
            )
        return event

    async def on_tool_completed(self, event: ToolCallCompleted) -> ToolCallCompleted:
        run_id = self._run_id(event.chat_id)
        if run_id is not None:
            await self._store.append_runtime_run_event(
                run_id,
                "tool.completed",
                {
                    "tool": event.tool_name,
                    "iteration": event.iteration,
                    "status": event.status,
                },
            )
        return event

    async def flush_run(self, run_id: UUID | str) -> None:
        parsed = run_id if isinstance(run_id, UUID) else UUID(str(run_id))
        async with self._lock(parsed):
            await self._flush_locked(parsed)

    async def close(self) -> None:
        for subscription in self._subscriptions:
            subscription.close()
        self._subscriptions.clear()
        for run_id in list(self._buffers):
            await self.flush_run(run_id)
        timers = list(self._timers.values())
        for timer in timers:
            timer.cancel()
        if timers:
            await asyncio.gather(*timers, return_exceptions=True)

    async def _delayed_flush(self, run_id: UUID) -> None:
        try:
            await asyncio.sleep(self._flush_interval)
            await self.flush_run(run_id)
        except asyncio.CancelledError:
            raise
        finally:
            current = asyncio.current_task()
            if self._timers.get(run_id) is current:
                self._timers.pop(run_id, None)

    async def _flush_locked(self, run_id: UUID) -> None:
        buffer = self._buffers.pop(run_id, None)
        timer = self._timers.pop(run_id, None)
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()
        if buffer is None or not buffer.content:
            return
        await self._store.append_runtime_run_event(
            run_id,
            "run.output.delta",
            {"content": buffer.content, "iteration": buffer.iteration},
        )

    def _lock(self, run_id: UUID) -> asyncio.Lock:
        return self._locks.setdefault(run_id, asyncio.Lock())

    @staticmethod
    def _run_id(value: str) -> UUID | None:
        try:
            return UUID(str(value))
        except ValueError:
            return None
