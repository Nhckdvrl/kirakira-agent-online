"""Drift 链路测试：skill 发现、连续性状态、端到端一轮 run。"""

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bus.queue import MessageBus
from plugins.drift_flow.runner import DriftRunner
from plugins.drift_flow.skills import discover_skills, ensure_example_skill
from plugins.drift_flow.state import DriftStateStore
from proactive_v2.config import DriftConfig
from core.schema import ModelResponse, ToolCall
from session.manager import SessionManager

NOW = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)


class SkillDiscoveryTests(unittest.TestCase):
    def test_ensure_and_discover_example(self):
        tmp = tempfile.TemporaryDirectory()
        workdir = Path(tmp.name)
        ensure_example_skill(workdir)
        skills = discover_skills(workdir)
        names = {s.name for s in skills}
        self.assertEqual(names, {"explore-curiosity", "review-memory"})
        self.assertTrue(all("finish_drift" in s.body for s in skills))
        tmp.cleanup()

    def test_plugin_skill_roots_are_discovered_and_duplicates_fail_loud(self):
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            plugin_root = workdir / "plugins" / "feeds" / "drift-skills"
            skill_dir = plugin_root / "follow-feed"
            skill_dir.mkdir(parents=True)
            skill_dir.joinpath("SKILL.md").write_text(
                "---\nname: follow-feed\ndescription: test\n---\nCall finish_drift.",
                encoding="utf-8",
            )
            skills = discover_skills(workdir, extra_roots=[plugin_root])
            self.assertEqual([skill.name for skill in skills], ["follow-feed"])

            duplicate = workdir / "drift" / "skills" / "follow-feed"
            duplicate.mkdir(parents=True)
            duplicate.joinpath("SKILL.md").write_text(
                "---\nname: follow-feed\ndescription: duplicate\n---\nbody",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate Drift skill"):
                discover_skills(workdir, extra_roots=[plugin_root])


class DriftStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = DriftStateStore(Path(self.tmp.name) / "drift.db")

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_min_interval_gating(self):
        self.assertTrue(self.store.can_run(NOW, 3.0))
        self.store.record_run(
            skill="s", now=NOW, status="completed", briefing="b", message_result="silent"
        )
        self.assertFalse(self.store.can_run(NOW + timedelta(hours=1), 3.0))
        self.assertTrue(self.store.can_run(NOW + timedelta(hours=4), 3.0))

    def test_continuum_roundtrip(self):
        self.store.save_continuum(
            skill="s", now=NOW, scratchpad="从第3步继续", next_tendency="想问音乐"
        )
        got = self.store.get_continuum("s")
        self.assertEqual(got["scratchpad"], "从第3步继续")
        self.assertEqual(got["next_tendency"], "想问音乐")


class _ScriptedClient:
    """驱动 agent loop：先 message_push，再 finish_drift，最后收尾。"""

    def __init__(self):
        self._step = 0
        self.tool_choices: list = []

    def complete(self, messages, tools, system, model, max_tokens, tool_choice=None):
        self.tool_choices.append(tool_choice)
        self._step += 1
        if self._step == 1:
            return ModelResponse(
                tool_calls=[
                    ToolCall(id="t1", name="message_push", arguments={"message": "最近在听什么歌？"})
                ]
            )
        if self._step == 2:
            return ModelResponse(
                tool_calls=[
                    ToolCall(
                        id="t2",
                        name="finish_drift",
                        arguments={"status": "completed", "briefing": "问了音乐话题"},
                    )
                ]
            )
        return ModelResponse(text="done", stop_reason="end_turn")


class DriftRunnerTests(unittest.TestCase):
    def test_end_to_end_run_pushes_and_records(self):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            workdir = Path(tmp.name)
            sessions = SessionManager(workdir)
            bus = MessageBus()
            sent = []
            client = _ScriptedClient()
            bus.subscribe_outbound("web", lambda m: sent.append(m) or asyncio.sleep(0))
            runner = DriftRunner(
                config=DriftConfig(enabled=True, min_interval_hours=0, max_steps=6),
                workspace=workdir,
                bus=bus,
                session_manager=sessions,
                model_client=client,
                model="fake",
                memory=None,
                target_channel="web",
                target_chat_id="u1",
            )
            dispatcher = asyncio.create_task(bus.dispatch_outbound())
            ran = await runner.maybe_run(NOW, "web:u1")
            await bus.drain(timeout=2)
            bus.stop()
            await dispatcher
            recent = runner._state.recent_runs()
            runner.close()
            sessions.close()
            tmp.cleanup()
            return ran, sent, recent, client.tool_choices

        ran, sent, recent, tool_choices = asyncio.run(scenario())
        self.assertTrue(ran)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0].content, "最近在听什么歌？")
        self.assertTrue(sent[0].metadata.get("drift"))
        self.assertTrue(sent[0].metadata.get("delivery_id"))
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["status"], "completed")
        self.assertEqual(recent[0]["message_result"], "sent")
        # 照 Reference drift 主循环:每步 tool_choice="required";
        # finish_drift 是收尾工具,执行后立即结束——不再有第三次模型调用。
        self.assertEqual(tool_choices, ["required", "required"])

    def test_disabled_does_not_run(self):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            workdir = Path(tmp.name)
            sessions = SessionManager(workdir)
            runner = DriftRunner(
                config=DriftConfig(enabled=False),
                workspace=workdir,
                bus=MessageBus(),
                session_manager=sessions,
                model_client=_ScriptedClient(),
                model="fake",
            )
            ran = await runner.maybe_run(NOW, "web:u1")
            runner.close()
            sessions.close()
            tmp.cleanup()
            return ran

        self.assertFalse(asyncio.run(scenario()))

    def test_channel_failure_records_silent_without_session_commit(self):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            workdir = Path(tmp.name)
            sessions = SessionManager(workdir)
            bus = MessageBus()

            async def fail_send(message):
                raise RuntimeError("channel unavailable")

            bus.subscribe_outbound("web", fail_send)
            runner = DriftRunner(
                config=DriftConfig(enabled=True, min_interval_hours=0, max_steps=6),
                workspace=workdir,
                bus=bus,
                session_manager=sessions,
                model_client=_ScriptedClient(),
                model="fake",
                target_channel="web",
                target_chat_id="u1",
            )
            dispatcher = asyncio.create_task(bus.dispatch_outbound())
            ran = await runner.maybe_run(NOW, "web:u1")
            recent = runner._state.recent_runs()
            session = sessions.get_or_create("web:u1")
            bus.stop()
            await dispatcher
            runner.close()
            sessions.close()
            tmp.cleanup()
            return ran, recent, session.messages

        ran, recent, messages = asyncio.run(scenario())
        self.assertTrue(ran)
        self.assertEqual(recent[0]["message_result"], "silent")
        self.assertFalse(any(m.get("drift") for m in messages))


if __name__ == "__main__":
    unittest.main()
