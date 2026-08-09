"""Drift 链路的持久状态（``drift.db``）。

保存每轮 run、每个 skill 的跨轮连续性（scratchpad / next_tendency）、以及
全局 min_interval 门控所需的 last_drift_at。参考 akashic 的
`plugins/drift_flow/state.py`，MVP 保留 run 记录 + skill 连续性 + 节流。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    skill TEXT NOT NULL,
    run_at TEXT NOT NULL,
    status TEXT NOT NULL,
    briefing TEXT NOT NULL DEFAULT '',
    message_result TEXT NOT NULL DEFAULT 'silent'
);
CREATE INDEX IF NOT EXISTS idx_runs_skill ON runs(skill, run_at);
CREATE TABLE IF NOT EXISTS drift_schedule (
    session_key TEXT PRIMARY KEY,
    timer_anchor TEXT NOT NULL DEFAULT '',
    next_attempt_at TEXT NOT NULL,
    sampled_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS skill_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    skill TEXT NOT NULL,
    entry_type TEXT NOT NULL,
    key TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    run_id INTEGER,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_skill_journal_skill_type_key
    ON skill_journal(skill, entry_type, key);
CREATE TABLE IF NOT EXISTS continuum (
    skill TEXT PRIMARY KEY,
    scratchpad TEXT NOT NULL DEFAULT '',
    next_tendency TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
"""


class DriftStateStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path))
        self._db.row_factory = sqlite3.Row
        self._db.executescript(_SCHEMA)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def last_drift_at(self) -> Optional[datetime]:
        row = self._db.execute(
            "SELECT run_at FROM runs ORDER BY run_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        try:
            return datetime.fromisoformat(row["run_at"])
        except ValueError:
            return None

    def can_run(self, now: datetime, min_interval_hours: float) -> bool:
        """距上次 Drift 不足 min_interval 时返回 False。"""
        if min_interval_hours <= 0:
            return True
        last = self.last_drift_at()
        if last is None:
            return True
        return (now - last).total_seconds() / 3600.0 >= min_interval_hours

    def last_run_at_by_skill(self) -> Dict[str, datetime]:
        rows = self._db.execute(
            "SELECT skill, MAX(run_at) AS last FROM runs GROUP BY skill"
        ).fetchall()
        out: Dict[str, datetime] = {}
        for row in rows:
            try:
                out[row["skill"]] = datetime.fromisoformat(row["last"])
            except (ValueError, TypeError):
                continue
        return out

    def record_run(
        self,
        *,
        skill: str,
        now: datetime,
        status: str,
        briefing: str,
        message_result: str,
    ) -> None:
        self._db.execute(
            """
            INSERT INTO runs (skill, run_at, status, briefing, message_result)
            VALUES (?, ?, ?, ?, ?)
            """,
            (skill, now.isoformat(), status, briefing, message_result),
        )
        self._db.commit()

    def recent_runs(self, limit: int = 10) -> List[dict]:
        rows = self._db.execute(
            """
            SELECT skill, run_at, status, briefing, message_result
            FROM runs ORDER BY run_at DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ── hazard 到期调度(照 Reference wake_proactive)─────────────────────────

    def load_schedule(self, session_key: str) -> Optional[dict]:
        row = self._db.execute(
            "SELECT timer_anchor, next_attempt_at, sampled_at FROM drift_schedule "
            "WHERE session_key = ?",
            (session_key,),
        ).fetchone()
        if row is None:
            return None
        try:
            next_at = datetime.fromisoformat(str(row["next_attempt_at"]))
        except ValueError:
            # 到期时刻损坏 → 当作没有排程,下一轮重新采样
            return None
        return {"timer_anchor": str(row["timer_anchor"] or ""), "next_attempt_at": next_at}

    def save_schedule(
        self, session_key: str, timer_anchor: str, next_attempt_at: datetime, now: datetime
    ) -> None:
        self._db.execute(
            """
            INSERT INTO drift_schedule (session_key, timer_anchor, next_attempt_at, sampled_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(session_key) DO UPDATE SET
                timer_anchor = excluded.timer_anchor,
                next_attempt_at = excluded.next_attempt_at,
                sampled_at = excluded.sampled_at
            """,
            (session_key, timer_anchor, next_attempt_at.isoformat(), now.isoformat()),
        )
        self._db.commit()

    def clear_schedule(self, session_key: str) -> None:
        """到期并真的跑了之后清掉,下一轮按新的空闲状态重新采样。"""
        self._db.execute("DELETE FROM drift_schedule WHERE session_key = ?", (session_key,))
        self._db.commit()

    # ── skill journal(照 Reference plugins/drift_flow/state.py)──────────────

    def append_journal(
        self,
        skill: str,
        entry_type: str,
        payload: dict,
        now: datetime,
        *,
        key: str = "",
        run_id: Optional[int] = None,
    ) -> None:
        """追加一条 journal。只增不改:Drift 的连续性来自"跑过什么"的完整记录。

        `entry_type` 区分种类,`self_observation` 是 Agent 对自己上一轮的观察;
        `key` 让同一类条目可以按主题归拢(如同一个待办的多次进展)。
        """
        clean_skill = str(skill or "").strip()
        clean_type = str(entry_type or "").strip()
        if not clean_skill or not clean_type:
            return
        self._db.execute(
            """
            INSERT INTO skill_journal (skill, entry_type, key, payload_json, run_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                clean_skill,
                clean_type,
                str(key or "").strip(),
                json.dumps(payload or {}, ensure_ascii=False),
                run_id,
                now.isoformat(),
            ),
        )
        self._db.commit()

    def load_journal(
        self,
        skill: str,
        *,
        entry_type: str = "",
        key: str = "",
        limit: int = 20,
    ) -> List[dict]:
        """按 skill(可选 type/key)取最近若干条,返回时间正序便于直接拼进 briefing。"""
        clean_skill = str(skill or "").strip()
        if not clean_skill:
            return []
        clauses = ["skill = ?"]
        params: List[object] = [clean_skill]
        if str(entry_type or "").strip():
            clauses.append("entry_type = ?")
            params.append(entry_type.strip())
        if str(key or "").strip():
            clauses.append("key = ?")
            params.append(key.strip())
        params.append(max(1, int(limit)))
        rows = self._db.execute(
            "SELECT id, entry_type, key, payload_json, run_id, created_at "
            "FROM skill_journal WHERE %s ORDER BY id DESC LIMIT ?" % " AND ".join(clauses),
            tuple(params),
        ).fetchall()
        return [self._journal_row(row) for row in reversed(rows)]

    def recent_self_observations(self, limit: int = 12) -> List[dict]:
        """跨 skill 的自我观察。Agent 借此看到"我最近这些轮干得怎么样"。"""
        rows = self._db.execute(
            "SELECT id, skill, entry_type, key, payload_json, run_id, created_at "
            "FROM skill_journal WHERE entry_type = 'self_observation' "
            "ORDER BY id DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
        return [
            {**self._journal_row(row), "skill": str(row["skill"] or "")}
            for row in reversed(rows)
        ]

    @staticmethod
    def _journal_row(row) -> dict:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except ValueError:
            # 脏数据不该让整段 journal 读不出来
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return {
            "id": int(row["id"] or 0),
            "entry_type": str(row["entry_type"] or ""),
            "key": str(row["key"] or ""),
            "payload": payload,
            "run_id": int(row["run_id"]) if row["run_id"] is not None else None,
            "created_at": str(row["created_at"] or ""),
        }

    def get_continuum(self, skill: str) -> dict:
        row = self._db.execute(
            "SELECT scratchpad, next_tendency FROM continuum WHERE skill = ?",
            (skill,),
        ).fetchone()
        if row is None:
            return {"scratchpad": "", "next_tendency": ""}
        return {"scratchpad": row["scratchpad"], "next_tendency": row["next_tendency"]}

    def save_continuum(
        self,
        *,
        skill: str,
        now: datetime,
        scratchpad: str = "",
        next_tendency: str = "",
    ) -> None:
        self._db.execute(
            """
            INSERT INTO continuum (skill, scratchpad, next_tendency, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(skill) DO UPDATE SET
                scratchpad = excluded.scratchpad,
                next_tendency = excluded.next_tendency,
                updated_at = excluded.updated_at
            """,
            (skill, scratchpad, next_tendency, now.isoformat()),
        )
        self._db.commit()
