"""Persistent user-requested delayed message scheduler."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone as dt_timezone
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.triggers.cron import CronTrigger

from bus.queue import MessageBus
from bus.events import InboundMessage, OutboundMessage
from core.schema import ToolSpec
from agent.tools.registry import ToolRegistry, object_schema


@dataclass
class ScheduledMessage:
    id: str
    channel: str
    chat_id: str
    message: str
    run_at: str
    interval_seconds: int = 0
    remaining_runs: int = 1
    status: str = "pending"
    created_at: str = ""
    last_error: str = ""
    tier: str = "instant"
    trigger: str = "at"
    cron_expr: str = ""
    timezone: str = "UTC"
    prompt: str = ""
    name: str = ""


_DURATION_PARTS = re.compile(
    r"^(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$"
)


def parse_duration(value: str) -> timedelta:
    matched = _DURATION_PARTS.fullmatch(value.strip())
    if matched is None or not any(matched.groups()):
        raise ValueError("invalid duration; examples: 30s, 5m, 2h, 1h30m")
    days, hours, minutes, seconds = (int(item or 0) for item in matched.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def is_cron_expr(value: str) -> bool:
    return len(value.strip().split()) in {5, 6}


def next_cron_fire(value: str, tz: str, after: datetime) -> datetime:
    parts = value.strip().split()
    if len(parts) not in {5, 6}:
        raise ValueError("cron expression must contain 5 or 6 fields")
    zone = ZoneInfo(tz)
    names = (
        ("minute", "hour", "day", "month", "day_of_week")
        if len(parts) == 5
        else ("second", "minute", "hour", "day", "month", "day_of_week")
    )
    try:
        trigger = CronTrigger(timezone=zone, **dict(zip(names, parts)))
        result = trigger.get_next_fire_time(None, after.astimezone(zone))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid cron expression: %s" % value) from exc
    if result is None:
        raise ValueError("cron expression has no next fire time: %s" % value)
    return result


def parse_when_at(value: str, tz: str, *, now: datetime | None = None) -> datetime:
    zone = ZoneInfo(tz)
    current = (now or datetime.now(zone)).astimezone(zone)
    text = value.strip()
    if re.fullmatch(r"\d{1,2}:\d{2}", text):
        parsed = datetime.strptime(text, "%H:%M")
        result = current.replace(
            hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0
        )
        return result + timedelta(days=1) if result <= current else result
    parsed = datetime.fromisoformat(text)
    return parsed.replace(tzinfo=zone) if parsed.tzinfo is None else parsed


class SchedulerService:
    MAX_ACTIVE_JOBS = 10

    def __init__(
        self,
        path: Path,
        *,
        bus: MessageBus,
        tools: ToolRegistry,
    ) -> None:
        self.path = path
        self.bus = bus
        self.tools = tools
        self._jobs: Dict[str, ScheduledMessage] = {}
        self._wake = asyncio.Event()
        self._running = False
        self._load()
        self._register_tools()

    async def run(self) -> None:
        self._running = True
        while self._running:
            due = self._due_jobs()
            for job in due:
                await self._fire(job)
            timeout = self._seconds_until_next()
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                continue

    def stop(self) -> None:
        self._running = False
        self._wake.set()

    async def schedule(
        self,
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
        channel: str = "",
        chat_id: str = "",
    ) -> str:
        context = self.tools.context
        channel = str(channel or context.get("channel") or "").strip()
        chat_id = str(chat_id or context.get("chat_id") or "").strip()
        if not channel or not chat_id:
            return "Error: schedule requires an active channel/chat context"
        tier = tier.strip().lower()
        if tier not in {"instant", "soft"}:
            return "Error: tier must be instant or soft"
        message = message.strip()
        prompt = prompt.strip()
        if tier == "instant" and not message:
            return "Error: message is required for tier=instant"
        if tier == "soft" and not prompt:
            return "Error: prompt is required for tier=soft"
        tz_name = timezone.strip() or str(context.get("timezone") or "UTC")
        try:
            zone = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            return "Error: invalid timezone: %s" % tz_name

        trigger = trigger.strip().lower()
        cron_expr = ""
        interval = max(0, int(interval_seconds))
        repeats = max(1, min(1000, int(repeat_count)))
        try:
            if trigger:
                if trigger not in {"at", "after", "every"}:
                    return "Error: trigger must be at, after, or every"
                if not when.strip():
                    return "Error: when is required with trigger"
                if trigger == "at":
                    scheduled_at = parse_when_at(when, tz_name)
                elif trigger == "after":
                    base = (
                        datetime.fromisoformat(request_time)
                        if request_time
                        else datetime.now(zone)
                    )
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
                        return "Error: every interval must be positive"
                    scheduled_at = datetime.now(zone) + duration
                    repeats = -1
            elif run_at:
                scheduled_at = parse_when_at(run_at, tz_name)
                trigger = "at"
            elif delay_seconds > 0:
                scheduled_at = datetime.now(zone) + timedelta(seconds=int(delay_seconds))
                trigger = "after"
            else:
                return "Error: provide trigger/when, run_at, or a positive delay_seconds"
        except (TypeError, ValueError, OverflowError) as exc:
            return "Error: %s" % exc

        when_dt = scheduled_at.astimezone(dt_timezone.utc)
        now_utc = datetime.now(dt_timezone.utc)
        if when_dt <= now_utc:
            return "Error: scheduled time must be in the future"
        if not trigger:
            trigger = "every" if interval > 0 else "at"
        if trigger != "every" and repeats > 1 and interval <= 0:
            return "Error: interval_seconds is required when repeat_count > 1"
        active_jobs = sum(job.status == "pending" for job in self._jobs.values())
        if active_jobs >= self.MAX_ACTIVE_JOBS:
            return (
                "Error: schedule_capacity_reached active=%d max=%d"
                % (active_jobs, self.MAX_ACTIVE_JOBS)
            )
        job = ScheduledMessage(
            id="job_%s" % uuid4().hex[:12],
            channel=channel,
            chat_id=chat_id,
            message=message,
            run_at=when_dt.isoformat(),
            interval_seconds=interval,
            remaining_runs=repeats,
            created_at=datetime.now().astimezone().isoformat(),
            tier=tier,
            trigger=trigger,
            cron_expr=cron_expr,
            timezone=tz_name,
            prompt=prompt,
            name=name.strip(),
        )
        self._jobs[job.id] = job
        self._save()
        self._wake.set()
        return json.dumps(asdict(job), ensure_ascii=False)

    def list_schedules(self, include_finished: bool = False) -> str:
        jobs = [
            asdict(job)
            for job in self._jobs.values()
            if include_finished or job.status == "pending"
        ]
        jobs.sort(key=lambda item: item["run_at"])
        return json.dumps(jobs, ensure_ascii=False, indent=2)

    def cancel_schedule(self, job_id: str) -> str:
        job = self._jobs.get(job_id)
        if job is None:
            return "Error: schedule not found: %s" % job_id
        if job.status != "pending":
            return "Error: schedule is already %s" % job.status
        job.status = "cancelled"
        self._save()
        self._wake.set()
        return "Cancelled schedule %s" % job_id

    async def _fire(self, job: ScheduledMessage) -> None:
        try:
            if job.tier == "soft":
                await self.bus.publish_inbound(
                    InboundMessage(
                        channel=job.channel,
                        sender="scheduler:%s" % job.id,
                        chat_id=job.chat_id,
                        content=job.prompt,
                        metadata={
                            "scheduled_job_id": job.id,
                            "session_key_override": "scheduler:%s" % job.id,
                            "omit_user_turn": True,
                            "skip_post_memory": True,
                            "skip_memory_retrieval": True,
                            "disabled_tools": [
                                "message_push",
                                "recall_memory",
                                "memorize",
                                "reinforce_memory",
                                "forget_memory",
                            ],
                        },
                    )
                )
            else:
                await self.bus.publish_outbound(
                    OutboundMessage(
                        channel=job.channel,
                        chat_id=job.chat_id,
                        content=job.message,
                        metadata={"scheduled_job_id": job.id},
                    )
                )
            if job.remaining_runs > 0:
                job.remaining_runs -= 1
            if job.trigger == "every" and job.cron_expr:
                job.run_at = next_cron_fire(
                    job.cron_expr,
                    job.timezone,
                    max(datetime.fromisoformat(job.run_at), datetime.now(dt_timezone.utc)),
                ).astimezone(dt_timezone.utc).isoformat()
            elif job.trigger == "every" and job.interval_seconds > 0:
                next_time = datetime.fromisoformat(job.run_at) + timedelta(
                    seconds=job.interval_seconds
                )
                now = datetime.now(dt_timezone.utc)
                while next_time <= now:
                    next_time += timedelta(seconds=job.interval_seconds)
                job.run_at = next_time.isoformat()
            elif job.remaining_runs > 0 and job.interval_seconds > 0:
                next_time = datetime.fromisoformat(job.run_at) + timedelta(
                    seconds=job.interval_seconds
                )
                now = datetime.now(dt_timezone.utc)
                while next_time <= now:
                    next_time += timedelta(seconds=job.interval_seconds)
                job.run_at = next_time.isoformat()
            else:
                job.status = "completed"
        except Exception as exc:
            job.last_error = str(exc)
            job.status = "failed"
        self._save()

    def _due_jobs(self) -> List[ScheduledMessage]:
        now = datetime.now().astimezone()
        return [
            job
            for job in self._jobs.values()
            if job.status == "pending" and datetime.fromisoformat(job.run_at) <= now
        ]

    def _seconds_until_next(self) -> float:
        pending = [
            datetime.fromisoformat(job.run_at)
            for job in self._jobs.values()
            if job.status == "pending"
        ]
        if not pending:
            return 60.0
        return max(0.05, min(60.0, (min(pending) - datetime.now().astimezone()).total_seconds()))

    def _register_tools(self) -> None:
        self.tools.register(
            ToolSpec(
                "schedule",
                "Schedule a message for the current channel/chat at an ISO time or after a delay.",
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
                        "channel": {"type": "string"},
                        "chat_id": {"type": "string"},
                    },
                    [],
                ),
            ),
            self.schedule,
        )
        self.tools.register(
            ToolSpec(
                "list_schedules",
                "List scheduled messages for all channels.",
                object_schema({"include_finished": {"type": "boolean"}}, []),
            ),
            self.list_schedules,
        )
        self.tools.register(
            ToolSpec(
                "cancel_schedule",
                "Cancel a pending scheduled message by id.",
                object_schema({"job_id": {"type": "string"}}, ["job_id"]),
            ),
            self.cancel_schedule,
        )

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
        changed = False
        for item in jobs:
            # 单行损坏只跳过该行;整份文件丢弃会把所有用户任务清零。
            if not isinstance(item, dict) or not item.get("id"):
                continue
            try:
                job = ScheduledMessage(**item)
            except TypeError:
                continue
            if self._recover_job(job):
                changed = True
            self._jobs[job.id] = job
        if changed:
            self._save()

    _MISFIRE_GRACE_SECONDS = 300

    def _recover_job(self, job: ScheduledMessage) -> bool:
        """启动时的 misfire 恢复(照 Reference scheduler.load_and_recover)。

        重复任务把 run_at 推进到未来(保持相位);一次性任务过期超过宽限期则标记
        missed 而不补发——离线一天后重启,不能把积压的定时消息一次性轰给用户。
        """
        if job.status != "pending":
            return False
        try:
            run_at = datetime.fromisoformat(job.run_at)
        except ValueError:
            job.status = "failed"
            job.last_error = "invalid run_at"
            return True
        now = datetime.now().astimezone()
        if run_at > now:
            return False
        if job.trigger == "every" and job.cron_expr:
            try:
                job.run_at = next_cron_fire(
                    job.cron_expr, job.timezone, now
                ).astimezone(dt_timezone.utc).isoformat()
            except (ValueError, ZoneInfoNotFoundError) as exc:
                job.status = "failed"
                job.last_error = str(exc)
            return True
        if job.interval_seconds > 0 and (
            job.remaining_runs > 0 or job.trigger == "every"
        ):
            next_time = run_at
            while next_time <= now:
                next_time += timedelta(seconds=job.interval_seconds)
            job.run_at = next_time.isoformat()
            return True
        if (now - run_at).total_seconds() > self._MISFIRE_GRACE_SECONDS:
            job.status = "missed"
            job.last_error = "missed while runtime was offline"
            return True
        return False

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(".%s.%s.tmp" % (self.path.name, uuid4().hex))
        try:
            temp.write_text(
                json.dumps(
                    {"jobs": [asdict(job) for job in self._jobs.values()]},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            os.replace(temp, self.path)
        finally:
            try:
                temp.unlink()
            except FileNotFoundError:
                pass
