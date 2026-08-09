"""跨崩溃投递去重(照 Reference proactive_v2/state.py 的 deliveries 表)。

要解决的具体问题:进程在"渠道发送成功"与"本地提交"之间崩溃,重启后会重复打扰。

做法是内容指纹 + 时间窗,而不是两阶段 outbox:
- 发送**前**落地投递意图 → 崩溃走不到任何提交,标记保留 → 重启后同内容命中去重;
- 渠道**明确失败**时撤销标记 → 下一轮仍可重试。

这个取舍的代价是"标记后立刻崩溃"会漏发这一条。对主动推送而言,重复打扰比偶尔漏发更伤。
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from plugins.wake_proactive.state import ProactiveStateStore


def _store(tmp: str) -> ProactiveStateStore:
    return ProactiveStateStore(Path(tmp) / "proactive.db")


NOW = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
KEY = "a" * 64


class DeliveryDedupTests(unittest.TestCase):
    def test_unmarked_delivery_is_not_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            self.assertFalse(store.is_delivery_duplicate("cli:1", KEY, 24, NOW))
            store.close()

    def test_marked_delivery_is_duplicate_within_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.mark_delivery("cli:1", KEY, NOW)
            self.assertTrue(
                store.is_delivery_duplicate("cli:1", KEY, 24, NOW + timedelta(hours=1))
            )
            store.close()

    def test_expired_mark_is_not_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.mark_delivery("cli:1", KEY, NOW)
            # 超出窗口后同样内容可以再发
            self.assertFalse(
                store.is_delivery_duplicate("cli:1", KEY, 24, NOW + timedelta(hours=25))
            )
            store.close()

    def test_dedup_is_scoped_per_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.mark_delivery("cli:1", KEY, NOW)
            self.assertFalse(store.is_delivery_duplicate("cli:2", KEY, 24, NOW))
            store.close()

    def test_mark_survives_reopen_which_is_the_crash_case(self) -> None:
        # 这条最关键:标记必须落盘,否则"跨崩溃"根本无从谈起
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "proactive.db"
            first = ProactiveStateStore(path)
            first.mark_delivery("cli:1", KEY, NOW)
            first.close()

            reopened = ProactiveStateStore(path)
            self.assertTrue(reopened.is_delivery_duplicate("cli:1", KEY, 24, NOW))
            reopened.close()

    def test_unmark_allows_retry_after_channel_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.mark_delivery("cli:1", KEY, NOW)
            store.unmark_delivery("cli:1", KEY)
            # 渠道明确失败后必须能重试
            self.assertFalse(store.is_delivery_duplicate("cli:1", KEY, 24, NOW))
            store.close()

    def test_unmark_missing_row_is_harmless(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.unmark_delivery("cli:1", KEY)
            store.close()

    def test_remark_refreshes_the_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.mark_delivery("cli:1", KEY, NOW)
            later = NOW + timedelta(hours=20)
            store.mark_delivery("cli:1", KEY, later)
            # 以最后一次为准:相对首次已过窗,相对末次仍在窗内
            self.assertTrue(
                store.is_delivery_duplicate("cli:1", KEY, 24, later + timedelta(hours=1))
            )
            store.close()

    def test_corrupt_timestamp_is_treated_as_not_delivered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.mark_delivery("cli:1", KEY, NOW)
            store._db.execute(
                "UPDATE deliveries SET sent_at = ? WHERE delivery_key = ?",
                ("not-a-timestamp", KEY),
            )
            store._db.commit()
            # 宁可重发一次,也不要因为脏数据永久静默
            self.assertFalse(store.is_delivery_duplicate("cli:1", KEY, 24, NOW))
            store.close()


if __name__ == "__main__":
    unittest.main()
