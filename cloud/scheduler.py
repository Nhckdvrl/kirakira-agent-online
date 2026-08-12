"""PostgreSQL-backed Cloud adapter for the canonical Scheduler semantics."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import json
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from agent.scheduler import is_cron_expr, next_cron_fire, parse_duration, parse_when_at
from agent.tools.registry import ToolRegistry, object_schema
from cloud.logging import safe_exception_summary
from cloud.models import ScheduledJob
from cloud.store import (
    CloudStore,
    StoreConflictError,
    StoreNotFoundError,
    StoreStateError,
)
from core.schema import ToolSpec


async def create_cloud_schedule(
    store: CloudStore,
    user_id: UUID,
    conversation_id: UUID,
    *,
    message: str = "",
    run_at: str = "",
    delay_seconds: int = 0,
    interval_seconds: int = 0,
    repeat_count: int = 1,
    tier: str = "instant",
    trigger: str = "",
    when: str = "",
    prompt: str = "",
    timezone: str = "",
    name: str = "",
    request_time: str = "",
) -> ScheduledJob:
    """Parse exactly the original Scheduler inputs, then persist the job."""
    tier = tier.strip().lower()
    if tier not in {"instant", "soft"}:
        raise ValueError("tier must be instant or soft")
    message = message.strip()
    prompt = prompt.strip()
    if tier == "instant" and not message:
        raise ValueError("message is required for tier=instant")
    if tier == "soft" and not prompt:
        raise ValueError("prompt is required for tier=soft")
    tz_name = timezone.strip() or "UTC"
    try:
        zone = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"invalid timezone: {tz_name}") from exc

    normalized_trigger = trigger.strip().lower()
    cron_expr = ""
    interval = max(0, int(interval_seconds))
    repeats = max(1, min(1000, int(repeat_count)))
    if normalized_trigger:
        if normalized_trigger not in {"at", "after", "every"}:
            raise ValueError("trigger must be at, after, or every")
        if not when.strip():
            raise ValueError("when is required with trigger")
        if normalized_trigger == "at":
            scheduled_at = parse_when_at(when, tz_name)
        elif normalized_trigger == "after":
            base = datetime.fromisoformat(request_time) if request_time else datetime.now(zone)
            if base.tzinfo is None:
                base = base.replace(tzinfo=zone)
            scheduled_at = base + parse_duration(when)
        elif is_cron_expr(when):
            cron_expr = when.strip()
            scheduled_at = next_cron_fire(cron_expr, tz_name, datetime.now(zone))
            repeats = -1
        else:
            duration = parse_duration(when)
            interval = int(duration.total_seconds())
            if interval <= 0:
                raise ValueError("every interval must be positive")
            scheduled_at = datetime.now(zone) + duration
            repeats = -1
    elif run_at:
        scheduled_at = parse_when_at(run_at, tz_name)
        normalized_trigger = "at"
    elif delay_seconds > 0:
        scheduled_at = datetime.now(zone) + timedelta(seconds=int(delay_seconds))
        normalized_trigger = "after"
    else:
        raise ValueError("provide trigger/when, run_at, or a positive delay_seconds")

    when_utc = scheduled_at.astimezone(UTC)
    if when_utc <= datetime.now(UTC):
        raise ValueError("scheduled time must be in the future")
    if not normalized_trigger:
        normalized_trigger = "every" if interval > 0 else "at"
    if normalized_trigger != "every" and repeats > 1 and interval <= 0:
        raise ValueError("interval_seconds is required when repeat_count > 1")
    return await store.create_scheduled_job(
        user_id,
        conversation_id,
        message=message,
        prompt=prompt,
        run_at=when_utc,
        interval_seconds=interval,
        remaining_runs=repeats,
        tier=tier,
        trigger=normalized_trigger,
        cron_expr=cron_expr,
        timezone=tz_name,
        name=name.strip(),
    )


def scheduled_job_dict(job: ScheduledJob) -> dict[str, Any]:
    return {
        "id": job.id,
        "conversation_id": str(job.conversation_id),
        "message": job.message,
        "prompt": job.prompt,
        "run_at": job.run_at.isoformat(),
        "interval_seconds": job.interval_seconds,
        "remaining_runs": job.remaining_runs,
        "status": job.status,
        "tier": job.tier,
        "trigger": job.trigger,
        "cron_expr": job.cron_expr,
        "timezone": job.timezone,
        "name": job.name,
        "last_error": job.last_error,
        "created_at": job.created_at.isoformat(),
    }


class CloudSchedulerTools:
    """Agent-facing schedule/list/cancel tools scoped by task-local Cloud identity."""

    def __init__(self, store: CloudStore, tools: ToolRegistry) -> None:
        self.store = store
        self.tools = tools
        self._register()

    def _scope(self) -> tuple[UUID, UUID]:
        context = self.tools.context
        try:
            return UUID(str(context["principal_id"])), UUID(str(context["chat_id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("schedule requires an active Cloud user/conversation") from exc

    async def schedule(self, **kwargs: Any) -> str:
        user_id, conversation_id = self._scope()
        kwargs.pop("channel", None)
        kwargs.pop("chat_id", None)
        try:
            job = await create_cloud_schedule(
                self.store, user_id, conversation_id, **kwargs
            )
        except (ValueError, StoreConflictError) as exc:
            return f"Error: {exc}"
        return json.dumps(scheduled_job_dict(job), ensure_ascii=False)

    async def list_schedules(self, include_finished: bool = False) -> str:
        user_id, _ = self._scope()
        jobs = await self.store.list_scheduled_jobs(
            user_id, include_finished=include_finished
        )
        return json.dumps(
            [scheduled_job_dict(job) for job in jobs], ensure_ascii=False, indent=2
        )

    async def cancel_schedule(self, job_id: str) -> str:
        user_id, _ = self._scope()
        try:
            job = await self.store.cancel_scheduled_job(user_id, job_id)
        except (StoreStateError, StoreNotFoundError) as exc:
            return f"Error: {exc}"
        return f"Cancelled schedule {job.id}"

    def _register(self) -> None:
        self.tools.register(
            ToolSpec(
                "schedule",
                "Schedule an instant message or isolated Agent prompt for this Cloud conversation.",
                object_schema(
                    {
                        "message": {"type": "string"},
                        "run_at": {"type": "string"},
                        "delay_seconds": {"type": "integer"},
                        "interval_seconds": {"type": "integer"},
                        "repeat_count": {"type": "integer"},
                        "tier": {"type": "string", "enum": ["instant", "soft"]},
                        "trigger": {"type": "string", "enum": ["at", "after", "every"]},
                        "when": {"type": "string"},
                        "prompt": {"type": "string"},
                        "timezone": {"type": "string"},
                        "name": {"type": "string"},
                        "request_time": {"type": "string"},
                    },
                    [],
                ),
            ),
            self.schedule,
        )
        self.tools.register(
            ToolSpec(
                "list_schedules",
                "List this user's Cloud schedules.",
                object_schema({"include_finished": {"type": "boolean"}}, []),
            ),
            self.list_schedules,
        )
        self.tools.register(
            ToolSpec(
                "cancel_schedule",
                "Cancel a pending Cloud schedule by id.",
                object_schema({"job_id": {"type": "string"}}, ["job_id"]),
            ),
            self.cancel_schedule,
        )


class CloudScheduleWorker:
    def __init__(
        self,
        store: CloudStore,
        *,
        worker_id: str,
        poll_interval_seconds: float = 1.0,
        lease_seconds: int = 60,
    ) -> None:
        self.store = store
        self.worker_id = worker_id[:200]
        self.poll_interval_seconds = max(0.1, poll_interval_seconds)
        self.lease_seconds = max(10, lease_seconds)
        self._stop_requested = asyncio.Event()

    async def run_once(self) -> bool:
        job = await self.store.claim_next_scheduled_job(
            self.worker_id, lease_seconds=self.lease_seconds
        )
        if job is None:
            return False
        token = str(job.fire_token or "")
        try:
            await self.store.deliver_scheduled_job(job.id, self.worker_id, token)
            remaining = job.remaining_runs - 1 if job.remaining_runs > 0 else job.remaining_runs
            next_at = _next_run(job)
            status = "pending" if next_at is not None and remaining != 0 else "completed"
            await self.store.finish_scheduled_job(
                job.id,
                self.worker_id,
                token,
                status=status,
                remaining_runs=remaining,
                next_run_at=next_at,
            )
        except StoreStateError as exc:
            if "active passive Run" in str(exc):
                await self.store.finish_scheduled_job(
                    job.id,
                    self.worker_id,
                    token,
                    status="pending",
                    remaining_runs=job.remaining_runs,
                    next_run_at=datetime.now(UTC) + timedelta(seconds=5),
                    error=str(exc),
                )
            else:
                await self.store.finish_scheduled_job(
                    job.id,
                    self.worker_id,
                    token,
                    status="failed",
                    remaining_runs=job.remaining_runs,
                    error=safe_exception_summary(exc),
                )
        except Exception as exc:  # noqa: BLE001 - durable terminal evidence
            await self.store.finish_scheduled_job(
                job.id,
                self.worker_id,
                token,
                status="failed",
                remaining_runs=job.remaining_runs,
                error=safe_exception_summary(exc),
            )
        return True

    async def run_forever(self) -> None:
        while not self._stop_requested.is_set():
            if await self.run_once():
                continue
            try:
                await asyncio.wait_for(
                    self._stop_requested.wait(), timeout=self.poll_interval_seconds
                )
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop_requested.set()


def _next_run(job: ScheduledJob) -> datetime | None:
    now = datetime.now(UTC)
    job_run_at = job.run_at
    if job_run_at.tzinfo is None:  # SQLite contract-test normalization
        job_run_at = job_run_at.replace(tzinfo=UTC)
    if job.trigger == "every" and job.cron_expr:
        return next_cron_fire(job.cron_expr, job.timezone, max(job_run_at, now)).astimezone(UTC)
    if (job.trigger == "every" or job.remaining_runs > 1) and job.interval_seconds > 0:
        next_at = job_run_at + timedelta(seconds=job.interval_seconds)
        while next_at <= now:
            next_at += timedelta(seconds=job.interval_seconds)
        return next_at
    return None
