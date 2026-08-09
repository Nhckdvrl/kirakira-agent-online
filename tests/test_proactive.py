"""主动推送链路测试：电量模型、契约、状态库、数据源、端到端 tick。"""

import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bus.queue import MessageBus
from bus.events import OutboundMessage
from plugins.wake_proactive import energy
from proactive_v2.config import ProactiveConfig
from proactive_v2.contracts import (
    normalize_alert,
    normalize_content,
    rank_alerts,
    rank_content,
)
from proactive_v2.loop import ProactiveLoop
from plugins.wake_proactive.sources import (
    FileInboxSource,
    SourceRegistry,
    build_file_inbox_registry,
)
from plugins.wake_proactive.state import ProactiveStateStore
from core.schema import ModelResponse
from session.manager import SessionManager

NOW = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)


class EnergyTests(unittest.TestCase):
    def test_energy_decays_over_time(self):
        fresh = energy.compute_energy(NOW, NOW)
        stale = energy.compute_energy(NOW - timedelta(hours=6), NOW)
        self.assertGreater(fresh, stale)
        self.assertEqual(energy.compute_energy(None, NOW), 0.0)

    def test_base_score_higher_when_idle_or_active(self):
        idle = energy.base_score(energy.compute_energy(NOW - timedelta(days=2), NOW), 0)
        active = energy.base_score(energy.compute_energy(NOW, NOW), 12)
        # 久没聊、或刚聊得热，都该给出较高冲动分
        self.assertGreater(idle, 0.5)
        self.assertGreater(active, 0.5)

    def test_high_score_yields_shorter_interval(self):
        rng = __import__("random").Random(0)
        short = energy.next_tick_from_score(0.9, tick_s1=2400, tick_s0=4800, rng=rng)
        long = energy.next_tick_from_score(0.05, tick_s1=2400, tick_s0=4800, rng=rng)
        self.assertLess(short, long)


class ContractTests(unittest.TestCase):
    def test_normalize_alert_and_content(self):
        alert = normalize_alert(
            {"_source": "s", "event_id": "a1", "title": "T", "content": "B", "severity": "high"}
        )
        self.assertEqual(alert.item_id, "s:a1")
        self.assertIn("severity=high", alert.to_prompt_line())
        content = normalize_content(
            {"_source": "s", "event_id": "c1", "title": "C", "url": "http://x"}
        )
        self.assertEqual(content.item_id, "s:c1")
        self.assertIn("url=http://x", content.to_prompt_line(0))


class RankingTests(unittest.TestCase):
    def test_rank_alerts_by_severity_then_recency(self):
        events = [
            {"severity": "low", "published_at": "2026-07-22T10:00:00+00:00"},
            {"severity": "high", "published_at": "2026-07-22T09:00:00+00:00"},
            {"severity": "medium", "published_at": "2026-07-22T11:00:00+00:00"},
        ]
        ranked = rank_alerts(events)
        self.assertEqual(ranked[0]["severity"], "high")
        self.assertEqual(ranked[-1]["severity"], "low")

    def test_rank_content_newest_first(self):
        events = [
            {"first_seen_at": "2026-07-20T00:00:00+00:00", "title": "old"},
            {"first_seen_at": "2026-07-22T00:00:00+00:00", "title": "new"},
        ]
        self.assertEqual(rank_content(events)[0]["title"], "new")


class StateStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = ProactiveStateStore(Path(self.tmp.name) / "proactive.db")

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_ingest_dedups_and_unread_consume(self):
        events = [{"item_id": "s:1", "_source": "s", "event_id": "1", "title": "x"}]
        self.assertEqual(self.store.ingest("content", events, NOW), ["s:1"])
        # 第二次相同事件不再算新
        self.assertEqual(self.store.ingest("content", events, NOW), [])
        unread = self.store.unread("content")
        self.assertEqual(len(unread), 1)
        self.store.consume(["s:1"], NOW)
        self.assertEqual(self.store.unread("content"), [])

    def test_expire_old_content(self):
        old = [{"item_id": "s:old", "_source": "s", "event_id": "old", "title": "x"}]
        # first_seen = 20 天前
        self.store.ingest("content", old, NOW - timedelta(days=20))
        self.assertEqual(len(self.store.unread("content")), 1)
        expired = self.store.expire_old("content", NOW, 14.0)
        self.assertEqual(expired, 1)
        self.assertEqual(self.store.unread("content"), [])

    def test_cooldown(self):
        self.assertFalse(self.store.in_cooldown("k", NOW, 1.0))
        self.store.mark_push("k", NOW)
        self.assertTrue(self.store.in_cooldown("k", NOW + timedelta(minutes=30), 1.0))
        self.assertFalse(self.store.in_cooldown("k", NOW + timedelta(hours=2), 1.0))

    def test_consume_and_queue_ack_is_persistent_until_marked(self):
        events = [{"item_id": "s:1", "_source": "s", "event_id": "1"}]
        self.store.ingest("alert", events, NOW)
        self.store.consume_and_queue_ack(["s:1"], {"s": ["1"]}, NOW)
        self.assertEqual(self.store.unread("alert"), [])
        self.assertEqual(self.store.pending_acknowledgements(), {"s": ["1"]})
        self.store.mark_acknowledged("s", ["1"])
        self.assertEqual(self.store.pending_acknowledgements(), {})


class SourceTests(unittest.TestCase):
    def test_file_inbox_fetch_and_ack(self):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            inbox = Path(tmp.name) / "inbox"
            inbox.mkdir(parents=True)
            (inbox / "demo.jsonl").write_text(
                json.dumps({"kind": "alert", "event_id": "a1", "title": "T"}) + "\n"
                + json.dumps({"kind": "content", "event_id": "c1", "title": "C"}) + "\n",
                encoding="utf-8",
            )
            source = FileInboxSource("demo", inbox, ("alert", "content", "context"))
            events = await source.fetch()
            self.assertEqual(len(events), 2)
            await source.ack(["a1"])
            remaining = await source.fetch()
            self.assertEqual([e["event_id"] for e in remaining], ["c1"])
            registry = SourceRegistry()
            registry.add(source)
            grouped = await registry.fetch_all()
            self.assertEqual(len(grouped["content"]), 1)
            self.assertEqual(grouped["content"][0]["_source"], "demo")
            tmp.cleanup()

        asyncio.run(scenario())

    def test_build_file_inbox_registry_creates_dir(self):
        tmp = tempfile.TemporaryDirectory()
        registry = build_file_inbox_registry(Path(tmp.name))
        self.assertTrue((Path(tmp.name) / "proactive" / "inbox" / "README.md").exists())
        self.assertIsInstance(registry, SourceRegistry)
        tmp.cleanup()


class _FakeClient:
    """按 system prompt 返回固定 JSON 决策的假模型。"""

    def __init__(self, alert_msg="ALERT!", content_decision=None):
        self.alert_msg = alert_msg
        self.content_decision = content_decision or {"decision": "skip"}

    def complete(self, messages, tools, system, model, max_tokens):
        prompt = messages[0]["content"]
        if "【通道】alert" in prompt:
            return ModelResponse(text=json.dumps({"message": self.alert_msg}))
        return ModelResponse(text=json.dumps(self.content_decision))


class _FlakyAckSource:
    id = "flaky"
    channels = ("alert", "content", "context")

    def __init__(self):
        self.fail_ack = True
        self.acked = []

    async def fetch(self):
        if "a1" in self.acked:
            return []
        return [{"kind": "alert", "event_id": "a1", "title": "meeting"}]

    async def ack(self, event_ids):
        if self.fail_ack:
            raise RuntimeError("source unavailable")
        self.acked.extend(event_ids)


def _cfg(**kw):
    base = dict(enabled=True, channel="web", chat_id="u1", content_limit=5)
    base.update(kw)
    return ProactiveConfig(**base)


class LoopTickTests(unittest.TestCase):
    def _run_loop_once(self, inbox_events, client, drift_calls):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            workdir = Path(tmp.name)
            sessions = SessionManager(workdir)
            inbox = workdir / "proactive" / "inbox"
            inbox.mkdir(parents=True)
            (inbox / "demo.jsonl").write_text(
                "\n".join(json.dumps(e) for e in inbox_events) + "\n", encoding="utf-8"
            )
            sources = build_file_inbox_registry(workdir)
            state = ProactiveStateStore(workdir / "proactive.db")
            bus = MessageBus()
            sent = []
            bus.subscribe_outbound("web", lambda m: sent.append(m) or asyncio.sleep(0))

            async def drift_hook(now, key):
                drift_calls.append(key)
                return True

            loop = ProactiveLoop(
                config=_cfg(),
                bus=bus,
                session_manager=sessions,
                model_client=client,
                sources=sources,
                state=state,
                memory=None,
                drift_hook=drift_hook,
            )
            dispatcher = asyncio.create_task(bus.dispatch_outbound())
            await loop._tick()
            await bus.drain(timeout=2)
            bus.stop()
            await dispatcher
            loop.close()
            state.close()
            sessions.close()
            tmp.cleanup()
            return sent

        return asyncio.run(scenario())

    def test_alert_is_pushed(self):
        drift_calls = []
        sent = self._run_loop_once(
            [{"kind": "alert", "event_id": "a1", "title": "meeting", "content": "soon"}],
            _FakeClient(alert_msg="会议快开始了"),
            drift_calls,
        )
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0].content, "会议快开始了")
        self.assertTrue(sent[0].metadata.get("proactive"))
        self.assertEqual(drift_calls, [])  # 推了 alert 就不进 drift

    def test_content_skip_falls_through_to_drift(self):
        drift_calls = []
        sent = self._run_loop_once(
            [{"kind": "content", "event_id": "c1", "title": "news", "url": "http://x"}],
            _FakeClient(content_decision={"decision": "skip"}),
            drift_calls,
        )
        self.assertEqual(sent, [])
        self.assertEqual(drift_calls, ["web:u1"])  # 没推 → 交给 drift

    def test_content_send(self):
        drift_calls = []
        sent = self._run_loop_once(
            [{"kind": "content", "event_id": "c1", "title": "news", "url": "http://x"}],
            _FakeClient(
                content_decision={
                    "decision": "send",
                    "message": "看到条新闻",
                    "cited_ids": ["demo:c1"],
                }
            ),
            drift_calls,
        )
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0].content, "看到条新闻")
        self.assertTrue(sent[0].metadata.get("delivery_id"))
        self.assertEqual(drift_calls, [])

    def test_selected_content_feedback_is_persisted_and_sent_to_source(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                sessions = SessionManager(workdir)
                inbox = workdir / "proactive" / "inbox"
                inbox.mkdir(parents=True)
                (inbox / "demo.jsonl").write_text(
                    json.dumps(
                        {"kind": "content", "event_id": "c1", "title": "news"}
                    )
                    + "\n",
                    encoding="utf-8",
                )
                state = ProactiveStateStore(workdir / "proactive.db")
                bus = MessageBus()
                bus.subscribe_outbound("web", lambda _m: asyncio.sleep(0))
                loop = ProactiveLoop(
                    config=_cfg(),
                    bus=bus,
                    session_manager=sessions,
                    model_client=_FakeClient(
                        content_decision={
                            "decision": "send",
                            "message": "useful",
                            "cited_ids": ["demo:c1"],
                        }
                    ),
                    sources=build_file_inbox_registry(workdir),
                    state=state,
                )
                dispatcher = asyncio.create_task(bus.dispatch_outbound())

                await loop._tick()
                await bus.drain(timeout=2)

                feedback_rows = [
                    json.loads(line)
                    for line in (inbox / "demo.feedback.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                ]
                self.assertEqual(
                    feedback_rows,
                    [{"event_id": "c1", "feedback": "interesting"}],
                )
                self.assertEqual(state.pending_feedback(), [])
                bus.stop()
                await dispatcher
                loop.close()
                sessions.close()

        asyncio.run(scenario())

    def test_content_is_acked_on_ingest_but_kept_locally_when_skipped(self):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            workdir = Path(tmp.name)
            sessions = SessionManager(workdir)
            inbox = workdir / "proactive" / "inbox"
            inbox.mkdir(parents=True)
            (inbox / "demo.jsonl").write_text(
                json.dumps({"kind": "content", "event_id": "c1", "title": "news"})
                + "\n",
                encoding="utf-8",
            )
            state = ProactiveStateStore(workdir / "proactive.db")
            loop = ProactiveLoop(
                config=_cfg(),
                bus=MessageBus(),
                session_manager=sessions,
                model_client=_FakeClient(content_decision={"decision": "skip"}),
                sources=build_file_inbox_registry(workdir),
                state=state,
            )
            await loop._tick()
            self.assertEqual(state.unread_count("content"), 1)
            self.assertEqual(state.pending_acknowledgements(), {})
            self.assertIn(
                "c1", (inbox / "demo.acked").read_text(encoding="utf-8")
            )
            loop.close()
            sessions.close()
            tmp.cleanup()

        asyncio.run(scenario())

    def test_failed_alert_delivery_keeps_unread_and_retries_before_ack(self):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            workdir = Path(tmp.name)
            sessions = SessionManager(workdir)
            inbox = workdir / "proactive" / "inbox"
            inbox.mkdir(parents=True)
            (inbox / "demo.jsonl").write_text(
                json.dumps(
                    {"kind": "alert", "event_id": "a1", "title": "meeting"}
                )
                + "\n",
                encoding="utf-8",
            )
            state = ProactiveStateStore(workdir / "proactive.db")
            bus = MessageBus()
            sent = []
            fail_delivery = True

            async def channel_send(message):
                nonlocal fail_delivery
                if fail_delivery:
                    raise RuntimeError("channel unavailable")
                sent.append(message)

            bus.subscribe_outbound("web", channel_send)
            loop = ProactiveLoop(
                config=_cfg(),
                bus=bus,
                session_manager=sessions,
                model_client=_FakeClient(alert_msg="会议快开始了"),
                sources=build_file_inbox_registry(workdir),
                state=state,
            )
            dispatcher = asyncio.create_task(bus.dispatch_outbound())

            await loop._tick()
            self.assertEqual(state.unread_count("alert"), 1)
            self.assertFalse((inbox / "demo.acked").exists())
            self.assertEqual(sent, [])
            self.assertTrue(
                any(
                    d["action"] == "delivery_failed"
                    for d in state.recent_decisions()
                )
            )

            fail_delivery = False
            await loop._tick()
            self.assertEqual(len(sent), 1)
            self.assertEqual(state.unread_count("alert"), 0)
            self.assertIn("a1", (inbox / "demo.acked").read_text(encoding="utf-8"))
            session = sessions.get_or_create("web:u1")
            self.assertTrue(
                any(m.get("proactive") for m in session.messages)
            )

            bus.stop()
            await dispatcher
            loop.close()
            sessions.close()
            tmp.cleanup()

        asyncio.run(scenario())

    def test_source_ack_failure_stays_pending_and_flushes_next_tick(self):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            workdir = Path(tmp.name)
            sessions = SessionManager(workdir)
            state = ProactiveStateStore(workdir / "proactive.db")
            source = _FlakyAckSource()
            sources = SourceRegistry()
            sources.add(source)
            bus = MessageBus()
            sent = []
            bus.subscribe_outbound("web", lambda m: sent.append(m) or asyncio.sleep(0))
            loop = ProactiveLoop(
                config=_cfg(),
                bus=bus,
                session_manager=sessions,
                model_client=_FakeClient(alert_msg="会议快开始了"),
                sources=sources,
                state=state,
            )
            dispatcher = asyncio.create_task(bus.dispatch_outbound())

            await loop._tick()
            self.assertEqual(len(sent), 1)
            self.assertEqual(state.unread_count("alert"), 0)
            self.assertEqual(
                state.pending_acknowledgements(), {"flaky": ["a1"]}
            )

            source.fail_ack = False
            await loop._tick()
            self.assertEqual(len(sent), 1)
            self.assertEqual(state.pending_acknowledgements(), {})
            self.assertEqual(source.acked, ["a1"])

            bus.stop()
            await dispatcher
            loop.close()
            sessions.close()
            tmp.cleanup()

        asyncio.run(scenario())


class StatusAndGateTests(unittest.TestCase):
    def _loop(self, workdir, *, busy=False, drift_hook=None):
        sessions = SessionManager(workdir)
        (workdir / "proactive" / "inbox").mkdir(parents=True, exist_ok=True)
        return ProactiveLoop(
            config=_cfg(),
            bus=MessageBus(),
            session_manager=sessions,
            model_client=_FakeClient(),
            sources=build_file_inbox_registry(workdir),
            state=ProactiveStateStore(workdir / "proactive.db"),
            memory=None,
            drift_hook=drift_hook,
            passive_busy_fn=(lambda key: True) if busy else None,
        ), sessions

    def test_status_shape(self):
        tmp = tempfile.TemporaryDirectory()
        loop, sessions = self._loop(Path(tmp.name))
        st = loop.status()
        for key in ("target", "energy", "base_score", "estimated_next_interval_s",
                    "unread_alert", "unread_content",
                    "recent_decisions", "sources"):
            self.assertIn(key, st)
        self.assertEqual(st["target"], "web:u1")
        loop.close()
        sessions.close()
        tmp.cleanup()

    def test_busy_gate_records_decision_and_skips_drift(self):
        async def scenario():
            tmp = tempfile.TemporaryDirectory()
            drift_calls = []

            async def hook(now, key):
                drift_calls.append(key)
                return True

            loop, sessions = self._loop(Path(tmp.name), busy=True, drift_hook=hook)
            await loop.tick_once()
            decisions = loop.status()["recent_decisions"]
            loop.close()
            sessions.close()
            tmp.cleanup()
            return drift_calls, decisions

        drift_calls, decisions = asyncio.run(scenario())
        self.assertEqual(drift_calls, [])  # 忙时不进 drift
        self.assertTrue(any(d["action"] == "gated" for d in decisions))

    def test_tick_step_trajectory_records_gate_and_skipped_modules(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                loop, sessions = self._loop(Path(tmp), busy=True)
                await loop.tick_once()
                trajectory = loop.status()["recent_ticks"][0]
                loop.close()
                sessions.close()
                return trajectory

        trajectory = asyncio.run(scenario())
        self.assertEqual(trajectory["status"], "completed")
        self.assertEqual(trajectory["terminal"], "passive_busy")
        self.assertEqual(trajectory["steps"][0]["slot"], "proactive.gate")
        self.assertEqual(trajectory["steps"][0]["status"], "completed")
        self.assertTrue(
            all(step["status"] == "skipped" for step in trajectory["steps"][1:])
        )


if __name__ == "__main__":
    unittest.main()
