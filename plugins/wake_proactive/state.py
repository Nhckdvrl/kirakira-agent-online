"""主动链路的持久状态（``proactive.db``）。

职责：事件去重/入库、未读、消费标记、待回源 ACK、
推送冷却时间。参考 akashic 的 `plugins/wake_proactive/state.py`，其中本地
consume 与 pending acknowledgement 同事务提交。
"""

from __future__ import annotations

import json
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Sequence

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    item_id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unread'
);
CREATE INDEX IF NOT EXISTS idx_events_channel_status
    ON events(channel, status);
CREATE TABLE IF NOT EXISTS pending_acknowledgements (
    source_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    queued_at TEXT NOT NULL,
    PRIMARY KEY(source_id, source_event_id)
);
CREATE TABLE IF NOT EXISTS push_state (
    session_key TEXT PRIMARY KEY,
    last_push_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    session_key TEXT NOT NULL,
    delivery_key TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    PRIMARY KEY (session_key, delivery_key)
);
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decided_at TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS source_feedback (
    source_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    feedback TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    queued_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY(source_id, source_event_id, feedback)
);
CREATE TABLE IF NOT EXISTS tick_log (
    tick_id TEXT PRIMARY KEY,
    session_key TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    terminal TEXT,
    status TEXT NOT NULL,
    step_count INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS tick_step_log (
    tick_id TEXT NOT NULL,
    step_index INTEGER NOT NULL,
    slot TEXT NOT NULL,
    status TEXT NOT NULL,
    terminal TEXT,
    duration_ms INTEGER NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(tick_id, step_index)
);
CREATE INDEX IF NOT EXISTS idx_tick_log_started ON tick_log(started_at DESC);
"""


class ProactiveStateStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path))
        self._db.row_factory = sqlite3.Row
        self._db.executescript(_SCHEMA)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def ingest(
        self,
        channel: str,
        events: Sequence[Dict[str, Any]],
        now: datetime,
    ) -> List[str]:
        """入库一批事件，返回本轮**新出现**的 item_id（已存在的忽略）。"""
        new_ids: List[str] = []
        for event in events:
            item_id = str(event.get("item_id") or "").strip()
            if not item_id:
                continue
            source_id = str(event.get("_source") or "").strip()
            source_event_id = str(
                event.get("event_id") or event.get("id") or ""
            ).strip()
            cursor = self._db.execute(
                """
                INSERT OR IGNORE INTO events
                    (item_id, channel, source_id, source_event_id,
                     payload, first_seen_at, status)
                VALUES (?, ?, ?, ?, ?, ?, 'unread')
                """,
                (
                    item_id,
                    channel,
                    source_id,
                    source_event_id,
                    json.dumps(event, ensure_ascii=False),
                    now.isoformat(),
                ),
            )
            if cursor.rowcount:
                new_ids.append(item_id)
        self._db.commit()
        return new_ids

    def unread(self, channel: str) -> List[Dict[str, Any]]:
        """返回未读事件（含 first_seen_at 注解）。"""
        rows = self._db.execute(
            """
            SELECT item_id, source_id, source_event_id, payload, first_seen_at
            FROM events
            WHERE channel = ? AND status = 'unread'
            ORDER BY first_seen_at ASC
            """,
            (channel,),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            event = json.loads(row["payload"])
            event["item_id"] = row["item_id"]
            event["_source"] = row["source_id"]
            event["first_seen_at"] = row["first_seen_at"]
            out.append(event)
        return out

    def consume(self, item_ids: Sequence[str], now: datetime) -> None:
        """把事件标记为已消费，之后不再进入未读队列。"""
        ids = [str(i) for i in item_ids if str(i).strip()]
        if not ids:
            return
        self._db.executemany(
            "UPDATE events SET status = 'consumed' WHERE item_id = ?",
            [(item_id,) for item_id in ids],
        )
        self._db.commit()

    def consume_and_queue_ack(
        self,
        item_ids: Sequence[str],
        acknowledgements: Dict[str, Sequence[str]],
        now: datetime,
    ) -> None:
        """Reference 同款提交边界：消费事件与排队 source ACK 原子落库。"""
        ids = [str(i).strip() for i in item_ids if str(i).strip()]
        pending = [
            (str(source_id).strip(), str(event_id).strip(), now.isoformat())
            for source_id, event_ids in acknowledgements.items()
            if str(source_id).strip()
            for event_id in event_ids
            if str(event_id).strip()
        ]
        if not ids and not pending:
            return
        try:
            self._db.execute("BEGIN")
            if ids:
                self._db.executemany(
                    "UPDATE events SET status = 'consumed' WHERE item_id = ?",
                    [(item_id,) for item_id in ids],
                )
            if pending:
                self._db.executemany(
                    """
                    INSERT OR IGNORE INTO pending_acknowledgements
                        (source_id, source_event_id, queued_at)
                    VALUES (?, ?, ?)
                    """,
                    pending,
                )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def queue_acknowledgements(
        self,
        acknowledgements: Dict[str, Sequence[str]],
        now: datetime,
    ) -> None:
        """按 Reference 在 content 入库后排队回源 ACK；本地 reservoir 仍保留未读副本。"""
        pending = [
            (str(source_id).strip(), str(event_id).strip(), now.isoformat())
            for source_id, event_ids in acknowledgements.items()
            if str(source_id).strip()
            for event_id in event_ids
            if str(event_id).strip()
        ]
        if not pending:
            return
        self._db.executemany(
            """
            INSERT OR IGNORE INTO pending_acknowledgements
                (source_id, source_event_id, queued_at)
            VALUES (?, ?, ?)
            """,
            pending,
        )
        self._db.commit()

    def pending_acknowledgements(self) -> Dict[str, List[str]]:
        rows = self._db.execute(
            """
            SELECT source_id, source_event_id
            FROM pending_acknowledgements
            ORDER BY queued_at ASC, source_id ASC, source_event_id ASC
            """
        ).fetchall()
        grouped: Dict[str, List[str]] = {}
        for row in rows:
            grouped.setdefault(row["source_id"], []).append(row["source_event_id"])
        return grouped

    def mark_acknowledged(
        self, source_id: str, source_event_ids: Sequence[str]
    ) -> None:
        ids = [str(i).strip() for i in source_event_ids if str(i).strip()]
        if not source_id or not ids:
            return
        self._db.executemany(
            """
            DELETE FROM pending_acknowledgements
            WHERE source_id = ? AND source_event_id = ?
            """,
            [(source_id, event_id) for event_id in ids],
        )
        self._db.commit()

    def queue_feedback(
        self,
        source_id: str,
        source_event_ids: Sequence[str],
        feedback: str,
        now: datetime,
        reason: str = "",
    ) -> None:
        rows = [
            (source_id, str(event_id).strip(), feedback, reason[:300], now.isoformat())
            for event_id in source_event_ids
            if source_id and str(event_id).strip() and feedback
        ]
        if not rows:
            return
        self._db.executemany(
            """
            INSERT INTO source_feedback
                (source_id, source_event_id, feedback, reason, queued_at, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
            ON CONFLICT(source_id, source_event_id, feedback) DO UPDATE SET
                reason = excluded.reason,
                queued_at = excluded.queued_at,
                status = 'pending'
            """,
            rows,
        )
        self._db.commit()

    def pending_feedback(self) -> List[Dict[str, str]]:
        rows = self._db.execute(
            """
            SELECT source_id, source_event_id, feedback, reason
            FROM source_feedback WHERE status = 'pending'
            ORDER BY queued_at, source_id, source_event_id
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_feedback_sent(
        self, source_id: str, source_event_id: str, feedback: str
    ) -> None:
        self._db.execute(
            """
            UPDATE source_feedback SET status = 'sent'
            WHERE source_id = ? AND source_event_id = ? AND feedback = ?
            """,
            (source_id, source_event_id, feedback),
        )
        self._db.commit()

    def record_tick_start(
        self, tick_id: str, session_key: str, started_at: datetime
    ) -> None:
        self._db.execute(
            """
            INSERT OR REPLACE INTO tick_log
                (tick_id, session_key, started_at, status)
            VALUES (?, ?, ?, 'running')
            """,
            (tick_id, session_key, started_at.isoformat()),
        )
        self._db.commit()

    def record_tick_step(
        self,
        tick_id: str,
        step_index: int,
        slot: str,
        status: str,
        duration_ms: int,
        *,
        terminal: str | None = None,
        error: str = "",
    ) -> None:
        self._db.execute(
            """
            INSERT OR REPLACE INTO tick_step_log
                (tick_id, step_index, slot, status, terminal, duration_ms, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (tick_id, step_index, slot, status, terminal, duration_ms, error[:500]),
        )
        self._db.commit()

    def record_tick_finish(
        self,
        tick_id: str,
        finished_at: datetime,
        terminal: str | None,
        status: str,
        step_count: int,
        error: str = "",
    ) -> None:
        self._db.execute(
            """
            UPDATE tick_log SET finished_at = ?, terminal = ?, status = ?,
                step_count = ?, error = ? WHERE tick_id = ?
            """,
            (finished_at.isoformat(), terminal, status, step_count, error[:500], tick_id),
        )
        self._db.commit()

    def recent_ticks(self, limit: int = 10) -> List[Dict[str, Any]]:
        rows = self._db.execute(
            """
            SELECT tick_id, session_key, started_at, finished_at, terminal,
                   status, step_count, error
            FROM tick_log ORDER BY started_at DESC LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            steps = self._db.execute(
                """
                SELECT step_index, slot, status, terminal, duration_ms, error
                FROM tick_step_log WHERE tick_id = ? ORDER BY step_index
                """,
                (item["tick_id"],),
            ).fetchall()
            item["steps"] = [dict(step) for step in steps]
            result.append(item)
        return result

    def expire_old(self, channel: str, now: datetime, max_age_days: float) -> int:
        """淘汰 first_seen 超过 max_age_days 的未读事件，防止未读队列无界增长。

        对齐 reference `_content_expired` 的绝对陈旧淘汰意图（MVP 只按龄期，不做
        admission floor 衰减判定）。返回淘汰条数。
        """
        if max_age_days <= 0:
            return 0
        cutoff = (now - timedelta(days=max_age_days)).isoformat()
        cursor = self._db.execute(
            """
            UPDATE events SET status = 'expired'
            WHERE channel = ? AND status = 'unread' AND first_seen_at < ?
            """,
            (channel, cutoff),
        )
        self._db.commit()
        return cursor.rowcount

    def last_push_at(self, session_key: str) -> datetime | None:
        row = self._db.execute(
            "SELECT last_push_at FROM push_state WHERE session_key = ?",
            (session_key,),
        ).fetchone()
        if row is None:
            return None
        return datetime.fromisoformat(row["last_push_at"])

    def mark_push(self, session_key: str, now: datetime) -> None:
        self._db.execute(
            """
            INSERT INTO push_state (session_key, last_push_at)
            VALUES (?, ?)
            ON CONFLICT(session_key) DO UPDATE SET last_push_at = excluded.last_push_at
            """,
            (session_key, now.isoformat()),
        )
        self._db.commit()

    # ── 投递去重(照 Reference proactive_v2/state.py)────────────────────────

    def is_delivery_duplicate(
        self,
        session_key: str,
        delivery_key: str,
        window_hours: int,
        now: datetime,
    ) -> bool:
        """窗口内是否已投递过同一内容。

        这是"跨崩溃不重复发送"的实际手段:进程若在渠道发送成功与本地提交之间崩溃,
        重启后同样的内容会算出同样的 delivery_key,这里命中即跳过。
        """
        row = self._db.execute(
            "SELECT sent_at FROM deliveries WHERE session_key = ? AND delivery_key = ?",
            (session_key, delivery_key),
        ).fetchone()
        if row is None:
            return False
        cutoff = now - timedelta(hours=max(int(window_hours), 1))
        try:
            sent_at = datetime.fromisoformat(str(row["sent_at"]))
        except ValueError:
            # 记录损坏时按"未投递"处理:宁可重发一次,也不要因为脏数据永久静默。
            logger.warning(
                "[proactive.state] deliveries.sent_at 无法解析 session=%s key=%s",
                session_key,
                delivery_key[:16],
            )
            return False
        if sent_at < cutoff:
            return False
        logger.info(
            "[proactive.state] 命中发送去重 session=%s key=%s sent_at=%s window_h=%d",
            session_key,
            delivery_key[:16],
            row["sent_at"],
            window_hours,
        )
        return True

    def mark_delivery(self, session_key: str, delivery_key: str, now: datetime) -> None:
        self._db.execute(
            """
            INSERT INTO deliveries (session_key, delivery_key, sent_at)
            VALUES (?, ?, ?)
            ON CONFLICT(session_key, delivery_key) DO UPDATE SET sent_at = excluded.sent_at
            """,
            (session_key, delivery_key, now.isoformat()),
        )
        self._db.commit()

    def unmark_delivery(self, session_key: str, delivery_key: str) -> None:
        """撤销投递标记。

        只在**渠道明确报告失败**时调用:这类失败没有送达,必须允许下一轮重试。
        进程崩溃走不到这里,标记因此保留——这正是跨崩溃去重生效的路径。
        """
        self._db.execute(
            "DELETE FROM deliveries WHERE session_key = ? AND delivery_key = ?",
            (session_key, delivery_key),
        )
        self._db.commit()

    def record_decision(self, now: datetime, action: str, detail: str = "") -> None:
        """记录一次 tick 的决策，供 status 回看与 demo 展示（可观测性）。"""
        self._db.execute(
            "INSERT INTO decisions (decided_at, action, detail) VALUES (?, ?, ?)",
            (now.isoformat(), action, detail[:300]),
        )
        self._db.commit()

    def recent_decisions(self, limit: int = 10) -> List[Dict[str, Any]]:
        rows = self._db.execute(
            "SELECT decided_at, action, detail FROM decisions ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def unread_count(self, channel: str) -> int:
        row = self._db.execute(
            "SELECT COUNT(*) AS n FROM events WHERE channel = ? AND status = 'unread'",
            (channel,),
        ).fetchone()
        return int(row["n"]) if row else 0

    def in_cooldown(
        self,
        session_key: str,
        now: datetime,
        cooldown_hours: float,
    ) -> bool:
        """距上次推送不足冷却窗口时返回 True（用于抑制 content 刷屏）。"""
        if cooldown_hours <= 0:
            return False
        last = self.last_push_at(session_key)
        if last is None:
            return False
        elapsed_hours = (now - last).total_seconds() / 3600.0
        return elapsed_hours < cooldown_hours
