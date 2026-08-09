"""Drift hazard drive 与到期采样(照 Reference plugins/wake_proactive/drift_drive.py)。

原本是固定 min_interval 门控("满 N 小时就跑"),体感像定时打卡。hazard 把它变成
"闲下来了才去做点事":空闲越久速率越高,有内容要推/刚跑过/在重复则压低。

关键:用**采样到期时刻**而不是轮询判阈——后者会让"检查得越频繁越容易触发"这种
采样假象混进来(Reference 原话:到期事件只负责开启一次判别)。
"""

from __future__ import annotations

import math
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bus.queue import MessageBus
from plugins.drift_flow.drive import advance_drift_drive, sample_drift_delay_hours
from plugins.drift_flow.runner import DriftRunner
from proactive_v2.config import DriftConfig
from session.manager import SessionManager

NOW = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)


def _ago(hours: float) -> datetime:
    return NOW - timedelta(hours=hours)


def _drive(**kwargs):
    base = dict(
        now=NOW,
        hazard=0.0,
        threshold=1.0,
        updated_at=_ago(1),
        last_user_at=_ago(24),
        last_drift_at=None,
        content_evidence=0.0,
    )
    base.update(kwargs)
    return advance_drift_drive(**base)


class HazardMathTests(unittest.TestCase):
    def test_rate_grows_with_idle_time(self) -> None:
        rates = [_drive(last_user_at=_ago(h)).rate for h in (0.5, 4, 24)]
        self.assertLess(rates[0], rates[1])
        self.assertLess(rates[1], rates[2])

    def test_no_idle_baseline_gives_no_drive(self) -> None:
        self.assertEqual(_drive(last_user_at=None).idle_drive, 0.0)

    def test_content_evidence_suppresses(self) -> None:
        self.assertLess(_drive(content_evidence=0.9).rate, _drive().rate)

    def test_recent_drift_suppresses(self) -> None:
        self.assertLess(_drive(last_drift_at=_ago(0.5)).rate, _drive().rate)

    def test_repetition_suppresses(self) -> None:
        self.assertLess(_drive(repetition=0.9).rate, _drive().rate)

    def test_hazard_decays_toward_the_rate_driven_level(self) -> None:
        # 从一个很高的 hazard 出发,长时间后应回落(半衰期 12h)
        decayed = _drive(hazard=10.0, updated_at=_ago(48)).hazard_after
        self.assertLess(decayed, 10.0)

    def test_decision_follows_threshold(self) -> None:
        self.assertEqual(_drive(hazard=10.0, threshold=0.1).decision, "attempt")
        self.assertEqual(_drive(hazard=0.0, threshold=99.0).decision, "idle")

    def test_reasons_explain_suppression(self) -> None:
        result = _drive(content_evidence=0.9, last_drift_at=_ago(0.1), repetition=0.9)
        self.assertIn("content_evidence", result.reasons)
        self.assertIn("recent_drift", result.reasons)
        self.assertIn("repetition", result.reasons)

    def test_inputs_are_clamped(self) -> None:
        # 越界输入不该产生越界抑制
        self.assertEqual(_drive(content_evidence=5.0).content_suppression, 1.0)
        self.assertEqual(_drive(repetition=-3.0).repetition_suppression, 0.0)


class SampledExpiryTests(unittest.TestCase):
    def _delay(self, **kwargs) -> float:
        base = dict(
            random_draw=0.5,
            idle_hours=24.0,
            recent_drift_suppression=0.0,
            repetition_suppression=0.0,
        )
        base.update(kwargs)
        return sample_drift_delay_hours(**base)

    def test_suppression_pushes_the_expiry_later(self) -> None:
        self.assertLess(self._delay(), self._delay(recent_drift_suppression=0.8))
        self.assertLess(self._delay(), self._delay(repetition_suppression=0.8))

    def test_larger_draw_means_later_expiry(self) -> None:
        self.assertLess(self._delay(random_draw=0.1), self._delay(random_draw=0.9))

    def test_delay_is_positive_and_finite(self) -> None:
        delay = self._delay()
        self.assertGreater(delay, 0.0)
        self.assertTrue(math.isfinite(delay))

    def test_degenerate_draw_does_not_hang(self) -> None:
        # random_draw=0 时目标量为 0,必须立即返回而不是在倍增里空转
        self.assertGreaterEqual(self._delay(random_draw=0.0), 0.0)


class HazardGateTests(unittest.TestCase):
    def _runner(self, tmp: Path, sessions: SessionManager) -> DriftRunner:
        return DriftRunner(
            config=DriftConfig(enabled=True, min_interval_hours=0, max_steps=3),
            workspace=tmp,
            bus=MessageBus(),
            session_manager=sessions,
            model_client=None,
            model="fake",
            target_channel="web",
            target_chat_id="u1",
        )

    def _with_user_message(self, sessions: SessionManager, minutes_ago: int) -> None:
        session = sessions.get_or_create("web:u1")
        session.add_message("user", "hi")
        session.messages[-1]["timestamp"] = (
            NOW - timedelta(minutes=minutes_ago)
        ).isoformat()
        sessions.save(session)

    def test_no_user_message_defers_to_min_interval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sessions = SessionManager(Path(tmp))
            runner = self._runner(Path(tmp), sessions)
            # 没有空闲基准时 hazard 无从计算,不该额外设卡
            self.assertTrue(runner._hazard_due(NOW, "web:u1"))
            runner.close()
            sessions.close()

    def test_first_observation_samples_and_waits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sessions = SessionManager(Path(tmp))
            self._with_user_message(sessions, minutes_ago=5)
            runner = self._runner(Path(tmp), sessions)

            self.assertFalse(runner._hazard_due(NOW, "web:u1"))
            schedule = runner._state.load_schedule("web:u1")
            self.assertIsNotNone(schedule)
            self.assertGreater(schedule["next_attempt_at"], NOW)
            runner.close()
            sessions.close()

    def test_fires_once_due_then_clears_the_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sessions = SessionManager(Path(tmp))
            self._with_user_message(sessions, minutes_ago=5)
            runner = self._runner(Path(tmp), sessions)
            runner._hazard_due(NOW, "web:u1")
            due = runner._state.load_schedule("web:u1")["next_attempt_at"]

            self.assertTrue(runner._hazard_due(due + timedelta(minutes=1), "web:u1"))
            # 跑过之后清排程,下一轮按新的空闲状态重新采样
            self.assertIsNone(runner._state.load_schedule("web:u1"))
            runner.close()
            sessions.close()

    def test_new_user_message_resamples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sessions = SessionManager(Path(tmp))
            self._with_user_message(sessions, minutes_ago=60)
            runner = self._runner(Path(tmp), sessions)
            runner._hazard_due(NOW, "web:u1")
            first = runner._state.load_schedule("web:u1")["next_attempt_at"]

            # 用户又说话 → 锚点变化 → 重新采样
            self._with_user_message(sessions, minutes_ago=1)
            self.assertFalse(runner._hazard_due(NOW, "web:u1"))
            second = runner._state.load_schedule("web:u1")["next_attempt_at"]
            self.assertNotEqual(first, second)
            runner.close()
            sessions.close()

    def test_schedule_survives_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sessions = SessionManager(Path(tmp))
            self._with_user_message(sessions, minutes_ago=5)
            runner = self._runner(Path(tmp), sessions)
            runner._hazard_due(NOW, "web:u1")
            stored = runner._state.load_schedule("web:u1")["next_attempt_at"]
            runner.close()

            reopened = self._runner(Path(tmp), sessions)
            # 重启不该丢掉已采样的到期时刻,否则每次重启都会重新推迟
            self.assertEqual(
                reopened._state.load_schedule("web:u1")["next_attempt_at"], stored
            )
            reopened.close()
            sessions.close()


if __name__ == "__main__":
    unittest.main()
