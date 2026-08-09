"""Persistent chat sessions with tool-chain aware history reconstruction."""

from __future__ import annotations

import asyncio
import json
import hashlib
import logging
import os
import re
import shutil
import sqlite3
import threading
from uuid import uuid4
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from agent.model_runtime.query_compaction import (
    build_replay_compaction_messages,
    parse_react_compaction,
)

JsonDict = Dict[str, Any]
logger = logging.getLogger(__name__)


def _safe_key(key: str) -> str:
    readable = re.sub(r"[^\w.-]", "_", key).strip("._") or "session"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
    return "%s--%s" % (readable[:96], digest)


def _legacy_safe_key(key: str) -> str:
    return re.sub(r"[^\w.-]", "_", key)


def _truncate_tool_result(value: object, limit: int = 10000) -> str:
    text = value if isinstance(value, str) else str(value)
    if len(text) <= limit:
        return text
    omitted = max(0, len(text) - limit)
    while True:
        marker = "…%d chars truncated…" % omitted
        keep = max(0, limit - len(marker))
        actual = len(text) - keep
        if actual == omitted:
            break
        omitted = actual
    head = keep // 2
    tail = keep - head
    clipped = text[:head] + marker + (text[-tail:] if tail else "")
    return "Total output lines: %d\n\n%s" % (len(text.splitlines()), clipped)


@dataclass
class Session:
    key: str
    messages: List[JsonDict] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().astimezone().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().astimezone().isoformat())
    metadata: JsonDict = field(default_factory=dict)
    last_consolidated: int = 0
    _reserved_message_id: str | None = field(default=None, repr=False, compare=False)

    def add_message(
        self,
        role: str,
        content: str,
        *,
        media: List[str] | None = None,
        **kwargs: Any,
    ) -> JsonDict:
        seq = (
            max((int(item.get("seq", -1)) for item in self.messages), default=-1) + 1
        )
        msg: JsonDict = {
            "id": self._reserved_message_id or uuid4().hex,
            "seq": seq,
            "role": role,
            "content": content,
            "timestamp": datetime.now().astimezone().isoformat(),
            **kwargs,
        }
        if media:
            msg["media"] = list(media)
        self.messages.append(msg)
        self._reserved_message_id = None
        self.updated_at = datetime.now().astimezone().isoformat()
        return msg

    def get_history(
        self,
        max_messages: int = 80,
        *,
        start_index: int | None = None,
    ) -> List[JsonDict]:
        if max_messages <= 0:
            selected = []
        elif start_index is not None:
            start = max(0, min(int(start_index), len(self.messages)))
            if start >= len(self.messages):
                return []
            while start > 0 and self.messages[start].get("role") != "user":
                start -= 1
            selected = self.messages[start:]
        else:
            selected = self.messages[-max_messages:]
        first_user = next(
            (index for index, msg in enumerate(selected) if msg.get("role") == "user"),
            None,
        )
        if first_user is None:
            return []
        selected = selected[first_user:]
        out: List[JsonDict] = []
        for msg in selected:
            role = msg.get("role")
            if role == "user":
                out.append({"role": "user", "content": msg.get("content", "")})
                continue
            if role != "assistant":
                continue
            tool_chain = list(msg.get("tool_chain") or [])
            replay_tool_chain = tool_chain
            raw_compaction = msg.get("react_compaction")
            if raw_compaction is not None:
                message_id = str(msg.get("id") or f"{self.key}:{msg.get('seq', '?')}")
                compaction = parse_react_compaction(
                    raw_compaction,
                    source=message_id,
                )
                if compaction.compacted_tool_groups > len(tool_chain):
                    raise ValueError(
                        "react_compaction.compacted_tool_groups 超过 tool_chain 长度: "
                        + message_id
                    )
                out.extend(
                    build_replay_compaction_messages(
                        compaction,
                        message_id=message_id,
                    )
                )
                replay_tool_chain = tool_chain[compaction.compacted_tool_groups :]
            for group in replay_tool_chain:
                calls = list(group.get("calls") or [])
                if not calls:
                    continue
                assistant_tool_message = {
                    "role": "assistant",
                    "content": group.get("text") or "",
                    "tool_calls": [
                            {
                                "id": call["call_id"],
                                "type": "function",
                                "function": {
                                    "name": call["name"],
                                    "arguments": call.get("arguments", {}),
                                },
                            }
                            for call in calls
                        ],
                }
                if group.get("reasoning_content"):
                    assistant_tool_message["reasoning_content"] = group[
                        "reasoning_content"
                    ]
                out.append(assistant_tool_message)
                for call in calls:
                    out.append(
                        {
                            "role": "tool",
                            "tool_call_id": call["call_id"],
                            "content": _truncate_tool_result(call.get("result", "")),
                        }
                    )
            if msg.get("content"):
                assistant_msg = {"role": "assistant", "content": msg.get("content", "")}
                reasoning = msg.get("reasoning_content") or msg.get("thinking")
                if reasoning:
                    assistant_msg["reasoning_content"] = reasoning
                out.append(assistant_msg)
        return out

    def to_json(self) -> JsonDict:
        return {
            "key": self.key,
            "messages": self.messages,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata,
            "last_consolidated": self.last_consolidated,
        }

    @classmethod
    def from_json(cls, data: JsonDict) -> "Session":
        session = cls(
            key=str(data.get("key") or ""),
            messages=list(data.get("messages") or []),
            created_at=str(data.get("created_at") or datetime.now().astimezone().isoformat()),
            updated_at=str(data.get("updated_at") or datetime.now().astimezone().isoformat()),
            metadata=dict(data.get("metadata") or {}),
            last_consolidated=int(data.get("last_consolidated") or 0),
        )
        for seq, message in enumerate(session.messages):
            message.setdefault("seq", seq)
            message.setdefault(
                "id",
                hashlib.sha256(
                    f"{session.key}\0{seq}".encode("utf-8")
                ).hexdigest()[:32],
            )
        return session


class SessionManager:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.session_dir = workspace / "sessions"
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self._cache: Dict[str, Session] = {}
        self._delete_callbacks: List[Callable[[str], None]] = []
        self._index_lock = threading.Lock()
        # SQLite 是会话与消息的唯一权威状态。sessions/*.json 只作为旧版一次性
        # 导入源和人类可读镜像，启动后绝不再从镜像恢复运行态。
        self._index = sqlite3.connect(
            str(workspace / "sessions.db"),
            check_same_thread=False,
        )
        self._fts_enabled = self._initialize_index()
        self._closed = False
        self._migrate_legacy_json_once()
        self._drop_legacy_index()

    def _drop_legacy_index(self) -> None:
        """删掉换路径前的旧索引库(sessions/message_index.sqlite3)。

        它是纯派生物,新库已在 __init__ 里从 JSON 全量重建过,留着只会让人误以为
        还有第二份真相。删不掉不影响运行,忽略即可。
        """
        for suffix in ("", "-wal", "-shm"):
            legacy = self.session_dir / ("message_index.sqlite3%s" % suffix)
            try:
                legacy.unlink(missing_ok=True)
            except OSError:
                pass

    def get_or_create(self, key: str) -> Session:
        if key in self._cache:
            return self._cache[key]
        with self._index_lock:
            row = self._index.execute(
                "SELECT created_at, updated_at, metadata_json, last_consolidated "
                "FROM sessions WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                session = Session(key=key)
            else:
                messages = self._load_message_rows(key)
                session = Session(
                    key=key,
                    messages=messages,
                    created_at=str(row[0]),
                    updated_at=str(row[1]),
                    metadata=dict(json.loads(str(row[2]))),
                    last_consolidated=int(row[3]),
                )
        self._cache[key] = session
        return session

    def session_exists(self, key: str) -> bool:
        """判断会话是否已经存在,不像 ``get_or_create`` 那样顺带创建。

        控制面用它区分"resume 一个已有 thread"和"thread 不存在"。
        """
        if key in self._cache:
            return True
        with self._index_lock:
            return (
                self._index.execute(
                    "SELECT 1 FROM sessions WHERE key = ?", (key,)
                ).fetchone()
                is not None
            )

    def get_session_meta(self, key: str) -> Optional[JsonDict]:
        """读取会话元数据;不存在时返回 None(= Reference SessionStore 同名方法)。"""
        if not self.session_exists(key):
            return None
        session = self.get_or_create(key)
        return {
            "key": session.key,
            "created_at": session.created_at,
            "updated_at": session.updated_at,
            "metadata": dict(session.metadata),
        }

    def peek_next_message_id(self, session_key: str) -> str:
        session = self.get_or_create(session_key)
        if session._reserved_message_id is None:
            session._reserved_message_id = uuid4().hex
        return session._reserved_message_id

    def save(self, session: Session) -> None:
        self._save_sqlite_append_only(session)
        # 保留只读兼容镜像；镜像写失败不能撤销已经 durable 的权威事务。
        path = self._path(session.key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(
            ".%s.%d.%s.tmp" % (path.name, os.getpid(), uuid4().hex)
        )
        payload = json.dumps(session.to_json(), ensure_ascii=False, indent=2)
        try:
            temp_path.write_text(payload, encoding="utf-8")
            os.replace(temp_path, path)
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

    async def save_async(self, session: Session) -> None:
        """Persist session without blocking the channel event loop."""

        await asyncio.to_thread(self.save, session)

    def search_messages(self, query: str, limit: int = 10) -> List[JsonDict]:
        needle = query.lower().strip()
        if not needle:
            return []
        with self._index_lock:
            rows = self._index.execute(
                "SELECT id, session_key, role, content, ts FROM messages "
                "WHERE lower(content) LIKE ? ORDER BY ts DESC LIMIT ?",
                (f"%{needle}%", max(1, int(limit))),
            ).fetchall()
        return [
            {
                "source_ref": str(row[0]),
                "session_key": str(row[1]),
                "role": str(row[2]),
                "content": str(row[3])[:500],
                "timestamp": str(row[4]),
            }
            for row in rows
        ]

    def fetch_messages(self, source_ref: str, context: int = 2) -> List[JsonDict]:
        with self._index_lock:
            target = self._index.execute(
                "SELECT session_key, seq FROM messages WHERE id = ?", (source_ref,)
            ).fetchone()
            # 兼容旧 source_ref=session:index，但返回稳定 message id。
            if target is None and ":" in source_ref:
                session_key, raw_seq = source_ref.rsplit(":", 1)
                try:
                    target = (session_key, int(raw_seq))
                except ValueError:
                    target = None
            if target is None:
                return []
            radius = max(0, int(context))
            rows = self._index.execute(
                "SELECT id, role, content, ts FROM messages "
                "WHERE session_key = ? AND seq BETWEEN ? AND ? ORDER BY seq",
                (str(target[0]), int(target[1]) - radius, int(target[1]) + radius),
            ).fetchall()
        return [
            {
                "source_ref": str(row[0]),
                "role": str(row[1]),
                "content": str(row[2]),
                "timestamp": str(row[3]),
            }
            for row in rows
        ]

    def list_sessions(self) -> List[JsonDict]:
        with self._index_lock:
            rows = self._index.execute(
                "SELECT s.key, s.created_at, s.updated_at, s.metadata_json, "
                "COUNT(m.id) FROM sessions AS s "
                "LEFT JOIN messages AS m ON m.session_key = s.key "
                "GROUP BY s.key ORDER BY s.updated_at DESC"
            ).fetchall()
        result = [
            {
                "key": str(row[0]),
                "created_at": str(row[1]),
                "updated_at": str(row[2]),
                "message_count": int(row[4]),
                "metadata": dict(json.loads(str(row[3]))),
            }
            for row in rows
        ]
        persisted = {str(item["key"]) for item in result}
        for session in self._cache.values():
            if session.key in persisted:
                continue
            result.append(
                {
                    "key": session.key,
                    "created_at": session.created_at,
                    "updated_at": session.updated_at,
                    "message_count": len(session.messages),
                    "metadata": dict(session.metadata),
                }
            )
        return sorted(result, key=lambda item: str(item["updated_at"]), reverse=True)

    def get_channel_metadata(self, channel: str) -> List[JsonDict]:
        """Expose the narrow metadata projection used by Reference channels."""

        prefix = f"{channel}:"
        return [
            {
                "chat_id": str(item["key"])[len(prefix):],
                "metadata": dict(item.get("metadata") or {}),
            }
            for item in self.list_sessions()
            if str(item.get("key") or "").startswith(prefix)
        ]

    def delete_session(self, key: str) -> bool:
        existed = self.session_exists(key)
        self._cache.pop(key, None)
        for path in (self._path(key), self._legacy_path(key)):
            if path.exists():
                path.unlink()
                existed = True
        with self._index_lock:
            self._index.execute("BEGIN IMMEDIATE")
            try:
                self._index.execute("DELETE FROM messages WHERE session_key = ?", (key,))
                self._index.execute("DELETE FROM session_admissions WHERE session_key = ?", (key,))
                self._index.execute("DELETE FROM sessions WHERE key = ?", (key,))
                self._index.commit()
            except BaseException:
                self._index.rollback()
                raise
        if existed:
            for callback in list(self._delete_callbacks):
                try:
                    callback(key)
                except Exception:
                    logger.exception("session delete callback failed for %s", key)
        return existed

    def delete_messages(self, message_ids: List[str]) -> int:
        """Physically delete explicit stable IDs as a user-authorized action.

        Normal ``save`` remains append-only. Callers must surface this method as
        a destructive operation; deleting rows does not renumber surviving seqs.
        """

        ids = tuple(dict.fromkeys(str(item).strip() for item in message_ids if str(item).strip()))
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self._index_lock:
            affected = {
                str(row[0])
                for row in self._index.execute(
                    f"SELECT DISTINCT session_key FROM messages WHERE id IN ({placeholders})",
                    ids,
                ).fetchall()
            }
            self._index.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._index.execute(
                    f"DELETE FROM messages WHERE id IN ({placeholders})", ids
                )
                now = datetime.now().astimezone().isoformat()
                self._index.executemany(
                    "UPDATE sessions SET updated_at = ? WHERE key = ?",
                    [(now, key) for key in affected],
                )
                self._index.commit()
            except BaseException:
                self._index.rollback()
                raise
        for key in affected:
            self._cache.pop(key, None)
        return max(0, int(cursor.rowcount))

    def on_delete(self, callback: Callable[[str], None]) -> None:
        if callback not in self._delete_callbacks:
            self._delete_callbacks.append(callback)

    def _all_sessions(self) -> List[Session]:
        return [self.get_or_create(str(item["key"])) for item in self.list_sessions()]

    def _path(self, key: str) -> Path:
        return self.session_dir / ("%s.json" % _safe_key(key))

    def _legacy_path(self, key: str) -> Path:
        return self.session_dir / ("%s.json" % _legacy_safe_key(key))

    def _initialize_index(self) -> bool:
        with self._index_lock:
            self._index.execute("PRAGMA journal_mode=WAL")
            self._index.execute("PRAGMA synchronous=FULL")
            self._index.execute("PRAGMA foreign_keys=ON")
            self._index.execute(
                "CREATE TABLE IF NOT EXISTS sessions ("
                "key TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
                "metadata_json TEXT NOT NULL, last_consolidated INTEGER NOT NULL DEFAULT 0)"
            )
            self._index.execute(
                "CREATE TABLE IF NOT EXISTS messages ("
                "id TEXT PRIMARY KEY, session_key TEXT NOT NULL, seq INTEGER NOT NULL, "
                "role TEXT NOT NULL, content TEXT NOT NULL, ts TEXT NOT NULL, "
                "extra_json TEXT NOT NULL DEFAULT '{}')"
            )
            columns = {
                str(row[1])
                for row in self._index.execute("PRAGMA table_info(messages)").fetchall()
            }
            if "extra_json" not in columns:
                self._index.execute(
                    "ALTER TABLE messages ADD COLUMN extra_json TEXT NOT NULL DEFAULT '{}'"
                )
            self._index.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_session_seq "
                "ON messages(session_key, seq)"
            )
            self._index.execute(
                "CREATE TABLE IF NOT EXISTS session_admissions ("
                "session_key TEXT PRIMARY KEY, owner_id TEXT NOT NULL, acquired_at TEXT NOT NULL)"
            )
            self._index.execute(
                "CREATE TABLE IF NOT EXISTS session_schema ("
                "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            self._index.execute(
                "INSERT OR REPLACE INTO session_schema(key, value) VALUES('version', '2')"
            )
            self._initialize_messages_fts()
            self._index.commit()
            return True

    def _initialize_messages_fts(self) -> None:
        """messages 的外部内容 FTS(照 Reference `session/store.py`)。

        akasha 的关键词 lane 直接查 `messages_fts`,所以表名、`content='messages'`
        外部内容模式与三个同步触发器都要一致。触发器让它随投影表增量维护,
        不必每次全量重扫。trigram 不可用时退回默认分词。
        """
        for tokenize in ("tokenize='trigram'", ""):
            try:
                self._index.execute(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5("
                    "content, content='messages', content_rowid='rowid'%s)"
                    % ((", " + tokenize) if tokenize else "")
                )
                break
            except sqlite3.OperationalError:
                self._index.rollback()
        else:
            return
        for statement in (
            "CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN "
            "INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content); END",
            "CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN "
            "INSERT INTO messages_fts(messages_fts, rowid, content) "
            "VALUES('delete', old.rowid, old.content); END",
            "CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN "
            "INSERT INTO messages_fts(messages_fts, rowid, content) "
            "VALUES('delete', old.rowid, old.content); "
            "INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content); END",
        ):
            self._index.execute(statement)
        self._index.commit()

    def _migrate_legacy_json_once(self) -> None:
        with self._index_lock:
            migrated = self._index.execute(
                "SELECT value FROM session_schema WHERE key = 'legacy_json_imported'"
            ).fetchone()
        if migrated is not None:
            return
        sessions: list[Session] = []
        for path in sorted(self.session_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if payload.get("key"):
                    sessions.append(Session.from_json(payload))
            except (OSError, ValueError, TypeError) as exc:
                raise RuntimeError("Session migration source is unreadable: %s" % path) from exc
        with self._index_lock:
            legacy_rows = self._index.execute(
                "SELECT id, session_key, seq, role, content, ts, extra_json "
                "FROM messages ORDER BY session_key, seq"
            ).fetchall()
        if not sessions and legacy_rows:
            grouped: dict[str, list[JsonDict]] = {}
            for row in legacy_rows:
                extra = dict(json.loads(str(row[6]) or "{}"))
                grouped.setdefault(str(row[1]), []).append(
                    {
                        "id": str(row[0]),
                        "seq": int(row[2]),
                        "role": str(row[3]),
                        "content": str(row[4]),
                        "timestamp": str(row[5]),
                        **extra,
                    }
                )
            for key, messages in grouped.items():
                created = str(messages[0].get("timestamp") or datetime.now().astimezone().isoformat())
                updated = str(messages[-1].get("timestamp") or created)
                sessions.append(
                    Session(key=key, messages=messages, created_at=created, updated_at=updated)
                )
        if sessions:
            backup = (
                self.workspace
                / ".kirakira"
                / "backups"
                / ("session-migration-%s" % uuid4().hex)
            )
            backup.mkdir(parents=True, exist_ok=False)
            with self._index_lock:
                backup_conn = sqlite3.connect(backup / "sessions.db")
                try:
                    self._index.backup(backup_conn)
                finally:
                    backup_conn.close()
            shutil.copytree(self.session_dir, backup / "sessions")
        with self._index_lock:
            if sessions:
                for statement in (
                    "DROP TRIGGER IF EXISTS messages_ai",
                    "DROP TRIGGER IF EXISTS messages_ad",
                    "DROP TRIGGER IF EXISTS messages_au",
                    "DROP TABLE IF EXISTS messages_fts",
                ):
                    self._index.execute(statement)
                self._index.commit()
            self._index.execute("BEGIN IMMEDIATE")
            try:
                # Legacy messages was a rebuildable JSON projection. Clear it only
                # while performing the one-time, backed-up authority transfer.
                if sessions:
                    self._index.execute("DELETE FROM messages")
                    self._index.execute("DELETE FROM sessions")
                for session in sessions:
                    self._insert_session_and_messages(session)
                self._index.execute(
                    "INSERT OR REPLACE INTO session_schema(key, value) "
                    "VALUES('legacy_json_imported', '1')"
                )
                self._index.commit()
            except BaseException:
                self._index.rollback()
                raise
            if sessions:
                self._initialize_messages_fts()

    def _load_message_rows(self, session_key: str) -> List[JsonDict]:
        rows = self._index.execute(
            "SELECT id, seq, role, content, ts, extra_json FROM messages "
            "WHERE session_key = ? ORDER BY seq",
            (session_key,),
        ).fetchall()
        messages: List[JsonDict] = []
        for row in rows:
            extra = dict(json.loads(str(row[5]) or "{}"))
            messages.append(
                {
                    "id": str(row[0]),
                    "seq": int(row[1]),
                    "role": str(row[2]),
                    "content": str(row[3]),
                    "timestamp": str(row[4]),
                    **extra,
                }
            )
        return messages

    @staticmethod
    def _message_row(session_key: str, message: JsonDict) -> tuple[object, ...]:
        extra = {
            key: value
            for key, value in message.items()
            if key not in {"id", "seq", "role", "content", "timestamp"}
        }
        return (
            str(message["id"]),
            session_key,
            int(message["seq"]),
            str(message.get("role") or ""),
            str(message.get("content") or ""),
            str(message.get("timestamp") or ""),
            json.dumps(extra, ensure_ascii=False, sort_keys=True),
        )

    def _insert_session_and_messages(self, session: Session) -> None:
        self._index.execute(
            "INSERT INTO sessions(key, created_at, updated_at, metadata_json, last_consolidated) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                session.key,
                session.created_at,
                session.updated_at,
                json.dumps(session.metadata, ensure_ascii=False, sort_keys=True),
                int(session.last_consolidated),
            ),
        )
        self._index.executemany(
            "INSERT INTO messages(id, session_key, seq, role, content, ts, extra_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [self._message_row(session.key, message) for message in session.messages],
        )

    def _save_sqlite_append_only(self, session: Session) -> None:
        normalized = Session.from_json(session.to_json())
        session.messages[:] = normalized.messages
        with self._index_lock:
            self._index.execute("BEGIN IMMEDIATE")
            try:
                existing = self._load_message_rows(session.key)
                if len(existing) > len(session.messages):
                    raise RuntimeError("正常消息保存不得删除已持久化消息")
                for index, persisted in enumerate(existing):
                    candidate = session.messages[index]
                    if self._message_row(session.key, persisted) != self._message_row(
                        session.key, candidate
                    ):
                        raise RuntimeError(
                            "正常消息保存不得更新已持久化消息: %s" % persisted["id"]
                        )
                self._index.execute(
                    "INSERT INTO sessions(key, created_at, updated_at, metadata_json, last_consolidated) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET updated_at=excluded.updated_at, "
                    "metadata_json=excluded.metadata_json, "
                    "last_consolidated=excluded.last_consolidated",
                    (
                        session.key,
                        session.created_at,
                        session.updated_at,
                        json.dumps(session.metadata, ensure_ascii=False, sort_keys=True),
                        int(session.last_consolidated),
                    ),
                )
                self._index.executemany(
                    "INSERT INTO messages(id, session_key, seq, role, content, ts, extra_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        self._message_row(session.key, message)
                        for message in session.messages[len(existing) :]
                    ],
                )
                self._index.commit()
            except BaseException:
                self._index.rollback()
                raise

    def acquire_admission(self, session_key: str) -> str:
        owner_id = uuid4().hex
        with self._index_lock:
            try:
                self._index.execute(
                    "INSERT INTO session_admissions(session_key, owner_id, acquired_at) "
                    "VALUES (?, ?, ?)",
                    (session_key, owner_id, datetime.now().astimezone().isoformat()),
                )
                self._index.commit()
            except sqlite3.IntegrityError as exc:
                self._index.rollback()
                raise RuntimeError("session 已被另一进程占用: %s" % session_key) from exc
        return owner_id

    def release_admission(self, session_key: str, owner_id: str) -> None:
        with self._index_lock:
            cursor = self._index.execute(
                "DELETE FROM session_admissions WHERE session_key = ? AND owner_id = ?",
                (session_key, owner_id),
            )
            self._index.commit()
        if cursor.rowcount != 1:
            raise RuntimeError("session admission owner 不匹配: %s" % session_key)

    @contextmanager
    def admission(self, session_key: str):
        owner_id = self.acquire_admission(session_key)
        try:
            yield owner_id
        finally:
            self.release_admission(session_key, owner_id)

    def close(self) -> None:
        with self._index_lock:
            if self._closed:
                return
            self._index.close()
            self._closed = True
