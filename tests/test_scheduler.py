"""Persistent delayed message scheduler tests."""

import asyncio
from datetime import datetime, timedelta
import json
import tempfile
import unittest
from pathlib import Path

from bus.queue import MessageBus
from agent.scheduler import SchedulerService
from agent.tools.registry import ToolRegistry


class SchedulerTests(unittest.TestCase):
    def test_cron_timezone_schedule_is_persistent_and_repeating(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                tools = ToolRegistry()
                tools.set_context(channel="telegram", chat_id="42")
                scheduler = SchedulerService(
                    Path(tmp) / "schedules.json", bus=MessageBus(), tools=tools
                )

                created = json.loads(
                    await scheduler.schedule(
                        message="daily",
                        tier="instant",
                        trigger="every",
                        when="0 9 * * *",
                        timezone="Asia/Tokyo",
                        name="morning",
                    )
                )

                self.assertEqual(created["cron_expr"], "0 9 * * *")
                self.assertEqual(created["timezone"], "Asia/Tokyo")
                self.assertEqual(created["remaining_runs"], -1)
                self.assertEqual(created["status"], "pending")

        asyncio.run(scenario())

    def test_soft_schedule_uses_isolated_session_and_disables_memory(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                bus = MessageBus()
                tools = ToolRegistry()
                tools.set_context(channel="web", chat_id="u1")
                scheduler = SchedulerService(
                    Path(tmp) / "schedules.json", bus=bus, tools=tools
                )
                created = json.loads(
                    await scheduler.schedule(
                        tier="soft",
                        prompt="summarize the latest status",
                        delay_seconds=60,
                    )
                )

                await scheduler._fire(scheduler._jobs[created["id"]])
                inbound = await asyncio.wait_for(bus.consume_inbound(), timeout=1)

                self.assertEqual(inbound.content, "summarize the latest status")
                self.assertEqual(
                    inbound.metadata["session_key_override"],
                    "scheduler:%s" % created["id"],
                )
                self.assertTrue(inbound.metadata["skip_post_memory"])
                self.assertTrue(inbound.metadata["skip_memory_retrieval"])
                self.assertIn("recall_memory", inbound.metadata["disabled_tools"])
                await bus.complete_inbound(inbound)

        asyncio.run(scenario())

    def test_workspace_schedule_capacity_is_enforced(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                tools = ToolRegistry()
                tools.set_context(channel="web", chat_id="u1")
                scheduler = SchedulerService(
                    Path(tmp) / "schedules.json", bus=MessageBus(), tools=tools
                )
                for index in range(scheduler.MAX_ACTIVE_JOBS):
                    result = await scheduler.schedule(
                        message="job %d" % index, delay_seconds=3600 + index
                    )
                    self.assertFalse(result.startswith("Error:"))

                rejected = await scheduler.schedule(
                    message="one too many", delay_seconds=7200
                )
                self.assertIn("schedule_capacity_reached", rejected)

        asyncio.run(scenario())

    def test_schedule_fires_to_bound_channel_and_persists_completion(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "schedules.json"
                bus = MessageBus()
                tools = ToolRegistry()
                tools.set_context(channel="telegram", chat_id="42")
                scheduler = SchedulerService(path, bus=bus, tools=tools)
                received = []

                async def collect(message):
                    received.append(message)
                    scheduler.stop()
                    bus.stop()

                bus.subscribe_outbound("telegram", collect)
                run_at = (datetime.now().astimezone() + timedelta(seconds=0.1)).isoformat()
                created = json.loads(
                    await scheduler.schedule("reminder", run_at=run_at)
                )
                tasks = [
                    asyncio.create_task(bus.dispatch_outbound()),
                    asyncio.create_task(scheduler.run()),
                ]
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=3)

                self.assertEqual(received[0].content, "reminder")
                self.assertEqual(received[0].chat_id, "42")
                payload = json.loads(path.read_text(encoding="utf-8"))
                job = next(item for item in payload["jobs"] if item["id"] == created["id"])
                self.assertEqual(job["status"], "completed")

        asyncio.run(scenario())

    def test_cancelled_schedule_does_not_fire(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                bus = MessageBus()
                tools = ToolRegistry()
                tools.set_context(channel="web", chat_id="x")
                scheduler = SchedulerService(Path(tmp) / "schedules.json", bus=bus, tools=tools)
                created = json.loads(
                    await scheduler.schedule("later", delay_seconds=60)
                )

                result = scheduler.cancel_schedule(created["id"])

                self.assertIn("Cancelled", result)
                self.assertEqual(scheduler._due_jobs(), [])

        asyncio.run(scenario())


class MisfireRecoveryTests(unittest.TestCase):
    """重启恢复(照 Reference load_and_recover):不把离线期间积压的消息一次性轰出去。"""

    def _write_jobs(self, path: Path, jobs: list[dict]) -> None:
        path.write_text(json.dumps({"jobs": jobs}), encoding="utf-8")

    def _job(self, **overrides) -> dict:
        base = {
            "id": "job_x",
            "channel": "web",
            "chat_id": "u",
            "message": "hi",
            "run_at": datetime.now().astimezone().isoformat(),
            "interval_seconds": 0,
            "remaining_runs": 1,
            "status": "pending",
            "created_at": "",
            "last_error": "",
        }
        base.update(overrides)
        return base

    def _boot(self, tmp: str, jobs: list[dict]) -> SchedulerService:
        path = Path(tmp) / "schedules.json"
        self._write_jobs(path, jobs)
        return SchedulerService(path, bus=MessageBus(), tools=ToolRegistry())

    def test_stale_one_shot_is_marked_missed_not_fired(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = (datetime.now().astimezone() - timedelta(hours=6)).isoformat()
            scheduler = self._boot(tmp, [self._job(run_at=old)])
            job = scheduler._jobs["job_x"]
            self.assertEqual(job.status, "missed")
            self.assertEqual(scheduler._due_jobs(), [])

    def test_recent_one_shot_within_grace_still_fires(self):
        with tempfile.TemporaryDirectory() as tmp:
            recent = (datetime.now().astimezone() - timedelta(seconds=30)).isoformat()
            scheduler = self._boot(tmp, [self._job(run_at=recent)])
            self.assertEqual(scheduler._jobs["job_x"].status, "pending")
            self.assertEqual(len(scheduler._due_jobs()), 1)

    def test_repeating_job_advances_to_future_without_backlog(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = (datetime.now().astimezone() - timedelta(hours=6)).isoformat()
            scheduler = self._boot(
                tmp,
                [self._job(run_at=old, interval_seconds=3600, remaining_runs=100)],
            )
            job = scheduler._jobs["job_x"]
            self.assertEqual(job.status, "pending")
            self.assertGreater(
                datetime.fromisoformat(job.run_at), datetime.now().astimezone()
            )
            # 积压的 6 次不补发
            self.assertEqual(scheduler._due_jobs(), [])

    def test_bad_row_is_skipped_without_dropping_others(self):
        with tempfile.TemporaryDirectory() as tmp:
            future = (datetime.now().astimezone() + timedelta(hours=1)).isoformat()
            scheduler = self._boot(
                tmp,
                [
                    {"id": "bad", "unexpected_field": True},
                    self._job(id="job_ok", run_at=future),
                ],
            )
            self.assertNotIn("bad", scheduler._jobs)
            self.assertIn("job_ok", scheduler._jobs)


if __name__ == "__main__":
    unittest.main()
