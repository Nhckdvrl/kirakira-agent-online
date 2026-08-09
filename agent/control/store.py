"""控制面 turn 的持久化 owner。

Reference 把 turn 表放在 `session/store.py` 里,和会话共用一个 SQLite 连接。
kirakira 的 `session.py` 是 JSON canonical + FTS 索引,没有对应的位置,因此这里
单独开一个 `control.db` 承担同一职责。**表结构、状态机、CAS 条件与错误语义逐条
对齐 Reference**(`_TURN_TRANSITIONS`、`transition_turn` 的 compare-and-set、
`create_turn` 的 queued 前置校验),只有连接归属不同。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from agent.control.errors import (
    TurnNotFoundError,
    TurnStateTransitionError,
)
from agent.control.models import (
    TurnError,
    TurnItem,
    TurnRecord,
    TurnStatus,
    TurnUsage,
    parse_rfc3339,
)

# 合法状态转换。照 Reference session/store.py:31——终态之间不可再跳转。
_TURN_TRANSITIONS = {
    TurnStatus.QUEUED: frozenset({TurnStatus.IN_PROGRESS, TurnStatus.CANCELLED}),
    TurnStatus.IN_PROGRESS: frozenset(
        {
            TurnStatus.COMPLETED,
            TurnStatus.INTERRUPTED,
            TurnStatus.FAILED,
            TurnStatus.CANCELLED,
        }
    ),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS turns (
    id             TEXT PRIMARY KEY,
    session_key    TEXT NOT NULL,
    status         TEXT NOT NULL,
    input_json     TEXT NOT NULL,
    items_json     TEXT NOT NULL,
    usage_json     TEXT,
    error_json     TEXT,
    final_response TEXT,
    created_at     TEXT NOT NULL,
    started_at     TEXT,
    completed_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_turns_session_created
    ON turns(session_key, created_at, id);
CREATE TABLE IF NOT EXISTS threads (
    key        TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    metadata   TEXT NOT NULL DEFAULT '{}'
);
"""


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class ControlStore:
    """turn 与 thread 元数据的唯一持久化 owner。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- thread 元数据 -------------------------------------------------

    def upsert_thread(self, key: str, metadata: dict[str, Any] | None = None) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO threads (key, created_at, updated_at, metadata)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    metadata = excluded.metadata
                """,
                (key, now, now, _dumps(dict(metadata or {}))),
            )
            self._conn.commit()

    def touch_thread(self, key: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE threads SET updated_at = ? WHERE key = ?",
                (datetime.now(UTC).isoformat(), key),
            )
            self._conn.commit()

    def get_thread_meta(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT key, created_at, updated_at, metadata FROM threads WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "key": str(row["key"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "metadata": json.loads(str(row["metadata"]) or "{}"),
        }

    def list_threads(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key FROM threads ORDER BY updated_at DESC, key DESC"
            ).fetchall()
        return [{"key": str(row["key"])} for row in rows]

    def delete_thread(self, key: str) -> bool:
        with self._lock:
            cursor = self._conn.execute("DELETE FROM threads WHERE key = ?", (key,))
            self._conn.execute("DELETE FROM turns WHERE session_key = ?", (key,))
            self._conn.commit()
        return int(cursor.rowcount or 0) > 0

    # ---- turn 生命周期 -------------------------------------------------

    def create_turn(self, record: TurnRecord) -> TurnRecord:
        """持久化一个 queued turn 并返回数据库中的正式记录。"""
        if record.status is not TurnStatus.QUEUED:
            raise TurnStateTransitionError("turn 创建时必须处于 queued 状态")
        if record.started_at is not None or record.completed_at is not None:
            raise TurnStateTransitionError(
                "queued turn 不得包含 started_at/completed_at"
            )
        if (
            record.usage is not None
            or record.error is not None
            or record.final_response is not None
        ):
            raise TurnStateTransitionError(
                "queued turn 不得包含 usage/error/final_response"
            )

        # 1. 写入前完成全部 JSON 编码,序列化失败时数据库保持不变。
        input_json = _dumps({"input": record.input, "metadata": record.metadata})
        items_json = _dumps([item.to_dict() for item in record.items])

        # 2. 单条 INSERT 建立不可变 turn identity。
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO turns (
                    id, session_key, status, input_json, items_json,
                    usage_json, error_json, final_response,
                    created_at, started_at, completed_at
                )
                VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, NULL, NULL)
                """,
                (
                    record.id,
                    record.thread_id,
                    record.status.value,
                    input_json,
                    items_json,
                    record.created_at.isoformat(),
                ),
            )
            self._conn.commit()
        stored = self.read_turn(record.id)
        if stored is None:
            raise RuntimeError(f"turn 创建后无法读取: {record.id}")
        return stored

    def read_turn(self, turn_id: str) -> TurnRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id, session_key, status, input_json, items_json,
                       usage_json, error_json, final_response,
                       created_at, started_at, completed_at
                FROM turns
                WHERE id = ?
                """,
                (turn_id,),
            ).fetchone()
        return self._row_to_turn(row) if row is not None else None

    def transition_turn(
        self,
        turn_id: str,
        *,
        expected_status: TurnStatus,
        status: TurnStatus,
        thread_id: str | None = None,
        items: list[TurnItem] | None = None,
        usage: TurnUsage | None = None,
        error: TurnError | None = None,
        final_response: str | None = None,
        now: datetime | None = None,
    ) -> TurnRecord:
        """用单条 CAS 更新 turn 状态,状态漂移时明确失败。"""
        expected_status = TurnStatus(expected_status)
        status = TurnStatus(status)
        allowed = _TURN_TRANSITIONS.get(expected_status, frozenset())
        if status not in allowed:
            raise TurnStateTransitionError(
                f"非法 turn 状态转换: {expected_status.value} -> {status.value}"
            )
        if status is TurnStatus.FAILED and error is None:
            raise TurnStateTransitionError("failed turn 必须包含 error")
        timestamp = now or datetime.now(UTC)
        if timestamp.tzinfo is None:
            raise ValueError("turn transition 时间必须包含时区")
        timestamp = timestamp.astimezone(UTC)

        # 1. 只更新本次调用明确拥有的终态字段。
        set_parts = ["status = ?"]
        params: list[object] = [status.value]
        if status is TurnStatus.IN_PROGRESS:
            set_parts.append("started_at = ?")
            params.append(timestamp.isoformat())
        if status.is_terminal:
            set_parts.append("completed_at = ?")
            params.append(timestamp.isoformat())
        if items is not None:
            set_parts.append("items_json = ?")
            params.append(_dumps([item.to_dict() for item in items]))
        if usage is not None:
            set_parts.append("usage_json = ?")
            params.append(_dumps(usage.to_dict()))
        if error is not None:
            set_parts.append("error_json = ?")
            params.append(_dumps(error.to_dict()))
        if final_response is not None:
            set_parts.append("final_response = ?")
            params.append(final_response)

        # 2. status 与可选 thread identity 共同构成 compare-and-set 条件。
        where_parts = ["id = ?", "status = ?"]
        params.extend([turn_id, expected_status.value])
        if thread_id is not None:
            where_parts.append("session_key = ?")
            params.append(thread_id)
        with self._lock:
            cursor = self._conn.execute(
                f"UPDATE turns SET {', '.join(set_parts)} "
                f"WHERE {' AND '.join(where_parts)}",
                tuple(params),
            )
            if cursor.rowcount != 1:
                current = self._conn.execute(
                    "SELECT session_key, status FROM turns WHERE id = ?", (turn_id,)
                ).fetchone()
                self._conn.rollback()
                if current is None:
                    raise TurnNotFoundError(f"turn 不存在: {turn_id}")
                if thread_id is not None and str(current["session_key"]) != thread_id:
                    raise TurnNotFoundError(f"turn 不属于 thread: {thread_id}/{turn_id}")
                raise TurnStateTransitionError(
                    f"turn CAS 失败,期望 {expected_status.value},"
                    f"实际 {current['status']}: {turn_id}"
                )
            self._conn.commit()

        # 3. 返回提交后可重读的正式记录。
        stored = self.read_turn(turn_id)
        if stored is None:
            raise RuntimeError(f"turn 转换后无法读取: {turn_id}")
        return stored

    def list_turns(self, thread_id: str, *, limit: int = 100) -> list[TurnRecord]:
        """按创建时间倒序读取一个 thread 的稳定 turn 页面。"""
        if limit <= 0 or limit > 200:
            raise ValueError("turn list limit 必须在 1..200")
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, session_key, status, input_json, items_json,
                       usage_json, error_json, final_response,
                       created_at, started_at, completed_at
                FROM turns
                WHERE session_key = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (thread_id, limit),
            ).fetchall()
        return [self._row_to_turn(row) for row in rows]

    def _row_to_turn(self, row: sqlite3.Row) -> TurnRecord:
        """在 SQLite 边界把 turn 行恢复成严格领域对象。"""
        turn_id = str(row["id"])
        payload = json.loads(str(row["input_json"]) or "{}")
        if not isinstance(payload, dict):
            raise ValueError(f"turn input 必须是 JSON object: {turn_id}")
        input_text = payload.get("input")
        metadata = payload.get("metadata")
        if not isinstance(input_text, str):
            raise ValueError(f"turn input.input 必须是字符串: {turn_id}")
        if not isinstance(metadata, dict):
            raise ValueError(f"turn input.metadata 必须是 JSON object: {turn_id}")
        raw_items = json.loads(str(row["items_json"]) or "[]")
        if not isinstance(raw_items, list):
            raise ValueError(f"turn items 必须是 JSON array: {turn_id}")
        usage_raw = row["usage_json"]
        error_raw = row["error_json"]
        created_at = parse_rfc3339(str(row["created_at"]), "created_at")
        if created_at is None:
            raise ValueError(f"turn created_at 缺失: {turn_id}")
        return TurnRecord(
            id=turn_id,
            thread_id=str(row["session_key"]),
            status=TurnStatus(str(row["status"])),
            input=input_text,
            metadata=cast(dict[str, Any], metadata),
            items=[TurnItem.from_dict(item) for item in raw_items],
            usage=(
                TurnUsage.from_dict(json.loads(str(usage_raw)))
                if usage_raw is not None
                else None
            ),
            error=(
                TurnError.from_dict(json.loads(str(error_raw)))
                if error_raw is not None
                else None
            ),
            final_response=cast("str | None", row["final_response"]),
            created_at=created_at,
            started_at=parse_rfc3339(row["started_at"], "started_at"),
            completed_at=parse_rfc3339(row["completed_at"], "completed_at"),
        )
