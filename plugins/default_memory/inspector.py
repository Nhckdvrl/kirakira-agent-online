"""检索回放记录(照 Reference `plugins/default_memory/plugin.py` + `dashboard.py`)。

回答一个用单测答不了的问题:**这一轮到底召回了什么、注入了什么、模型又主动查了什么。**
检索质量出问题时,没有这份记录就只能靠猜——"是没召回,还是召回了没注入,还是注入了模型没用"。

按 turn 聚合两类记录:

- `context_prepare`:被动 turn 开始时的自动检索(engine.query(intent="context"));
- `recall_memory`:模型在 turn 内主动调用 recall_memory 工具的结果。

与 Reference 的两点差异:

1. **命中项直接来自引擎的结构化结果**,不像 Reference 那样用正则从注入文本里反解
   `RagHitLog(...)` ——我们在写入点就拿得到 `MemoryQueryResult.records`,没必要绕。
2. **有大小上限**。Reference 是无界 append;长跑会把 workspace 撑大,所以这里超过阈值
   时截断保留最近部分(见 `_trim_if_needed`)。观测数据可丢,不值得为它做轮转归档。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)

# 超过这个大小就截断到 _KEEP_BYTES;观测数据不做轮转归档。
_MAX_BYTES = 8 * 1024 * 1024
_KEEP_BYTES = 4 * 1024 * 1024
_MAX_ITEMS_PER_RECORD = 40


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def turn_id_for(session_key: str, timestamp: str, content: str) -> str:
    """与 Reference 同式:同一轮的两类记录靠它归到一起。"""
    raw = "%s\n%s\n%s" % (session_key, timestamp, content)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


class RecallInspector:
    """检索回放的写入与读取。两端都在这里,格式只有一处定义。"""

    def __init__(self, workspace: Path, *, enabled: bool = True) -> None:
        self.path = Path(workspace) / "observe" / "recall_inspector.jsonl"
        self.enabled = enabled
        self._lock = threading.RLock()
        # 记住每个 session 当前 turn 的 id:工具调用发生在检索之后,靠它归到同一轮。
        self._active_turns: dict[str, str] = {}

    # ── 写入 ────────────────────────────────────────────────────────

    def record_context_prepare(
        self,
        *,
        session_key: str,
        channel: str,
        chat_id: str,
        user_text: str,
        timestamp: str,
        records: list[Any],
        text_block: str,
        trace: dict[str, Any] | None = None,
    ) -> str:
        """记一次自动检索。返回 turn_id,供同一轮的工具调用归组。"""
        turn = turn_id_for(session_key, timestamp, user_text)
        self._active_turns[session_key] = turn
        if not self.enabled:
            return turn
        items = [self._compact(record) for record in (records or [])[:_MAX_ITEMS_PER_RECORD]]
        self._append(
            {
                "kind": "context_prepare",
                "turn_id": turn,
                "session_key": session_key,
                "channel": channel,
                "chat_id": chat_id,
                "user_text": user_text[:2000],
                "timestamp": timestamp,
                "created_at": _now_iso(),
                "context_prepare": {
                    "count": len(items),
                    "injected": bool(text_block.strip()),
                    "injected_chars": len(text_block),
                    "items": items,
                    "trace": dict(trace or {}),
                },
            }
        )
        return turn

    def record_recall_memory(
        self,
        *,
        session_key: str,
        channel: str = "",
        chat_id: str = "",
        arguments: dict[str, Any] | None = None,
        result_text: str = "",
        status: str = "success",
    ) -> None:
        """记一次模型主动调用 recall_memory 的结果。"""
        if not self.enabled:
            return
        payload = self._safe_json(result_text)
        raw_items = payload.get("items")
        items = [
            self._compact_dict(item)
            for item in (raw_items if isinstance(raw_items, list) else [])[:_MAX_ITEMS_PER_RECORD]
            if isinstance(item, dict)
        ]
        self._append(
            {
                "kind": "recall_memory",
                # 没有在途 turn 时(如 Drift 线程内调用)单开一轮,不丢记录。
                "turn_id": self._active_turns.get(session_key) or turn_id_for(
                    session_key, _now_iso(), json.dumps(arguments or {}, ensure_ascii=False)
                ),
                "session_key": session_key,
                "channel": channel,
                "chat_id": chat_id,
                "timestamp": _now_iso(),
                "created_at": _now_iso(),
                "recall_memory": {
                    "arguments": dict(arguments or {}),
                    "status": status,
                    "count": len(items),
                    "items": items,
                },
            }
        )

    @staticmethod
    def _compact(record: Any) -> dict[str, Any]:
        """引擎的 MemoryRecord → 面板需要的最小形状。"""
        return {
            "id": str(getattr(record, "id", "") or ""),
            "memory_type": str(getattr(record, "kind", "") or ""),
            "summary": str(getattr(record, "summary", "") or "")[:400],
            "score": round(float(getattr(record, "score", 0.0) or 0.0), 4),
            "injected": bool(getattr(record, "injected", False)),
        }

    @staticmethod
    def _compact_dict(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(item.get("id") or ""),
            "memory_type": str(item.get("memory_type") or item.get("kind") or ""),
            "summary": str(item.get("summary") or item.get("content") or "")[:400],
            "score": item.get("score"),
        }

    @staticmethod
    def _safe_json(text: str) -> dict[str, Any]:
        try:
            value = json.loads(text or "{}")
        except (json.JSONDecodeError, TypeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _append(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        try:
            with self._lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
                self._trim_if_needed()
        except OSError as exc:  # noqa: BLE001 - 观测失败绝不能影响主链路
            logger.warning("[observe] 检索记录写入失败: %s", exc)

    def _trim_if_needed(self) -> None:
        """超限时保留最近 _KEEP_BYTES,从下一个完整行开始。"""
        try:
            if self.path.stat().st_size <= _MAX_BYTES:
                return
            with self.path.open("rb") as handle:
                handle.seek(-_KEEP_BYTES, os.SEEK_END)
                handle.readline()  # 丢掉可能被截断的半行
                tail = handle.read()
        except OSError:
            return
        temp = self.path.with_name(".%s.%s.tmp" % (self.path.name, uuid4().hex))
        try:
            temp.write_bytes(tail)
            os.replace(temp, self.path)
        except OSError:
            temp.unlink(missing_ok=True)

    # ── 读取 ────────────────────────────────────────────────────────

    def overview(self) -> dict[str, Any]:
        turns = self._collect_turns()
        return {
            "available": self.path.exists(),
            "enabled": self.enabled,
            "total": len(turns),
            "latest_at": turns[0]["timestamp"] if turns else None,
            "path": str(self.path),
        }

    def list_turns(
        self,
        *,
        session_key: str = "",
        q: str = "",
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        needle = q.strip().lower()
        turns = [
            item
            for item in self._collect_turns()
            if (not session_key or item["session_key"] == session_key)
            and (not needle or needle in item["user_text"].lower())
        ]
        size = max(1, min(page_size, 200))
        start = (max(1, page) - 1) * size
        return turns[start : start + size], len(turns)

    def get_turn(self, turn_id: str) -> dict[str, Any] | None:
        return next(
            (item for item in self._collect_turns() if item["turn_id"] == turn_id), None
        )

    def _collect_turns(self) -> list[dict[str, Any]]:
        turns: dict[str, dict[str, Any]] = {}
        for record in self._read_records():
            turn_key = str(record.get("turn_id") or "")
            if not turn_key:
                continue
            turn = turns.setdefault(
                turn_key,
                {
                    "turn_id": turn_key,
                    "session_key": str(record.get("session_key") or ""),
                    "channel": str(record.get("channel") or ""),
                    "chat_id": str(record.get("chat_id") or ""),
                    "user_text": str(record.get("user_text") or ""),
                    "timestamp": str(record.get("timestamp") or ""),
                    "context_prepare": None,
                    "recall_memory_calls": [],
                },
            )
            # 后到的记录补齐先到的空字段(工具调用记录里没有 user_text)。
            for key in ("session_key", "channel", "chat_id", "user_text", "timestamp"):
                if record.get(key) and not turn.get(key):
                    turn[key] = str(record.get(key) or "")
            if record.get("kind") == "context_prepare":
                turn["context_prepare"] = record.get("context_prepare") or {}
            elif record.get("kind") == "recall_memory":
                turn["recall_memory_calls"].append(record.get("recall_memory") or {})

        result = list(turns.values())
        for item in result:
            prepare = item.get("context_prepare") or {}
            calls = item.get("recall_memory_calls") or []
            item["context_prepare_count"] = int(prepare.get("count") or 0)
            item["injected"] = bool(prepare.get("injected"))
            item["recall_call_count"] = len(calls)
            item["recall_memory_count"] = sum(int(call.get("count") or 0) for call in calls)
        result.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
        return result

    def tool_hook(self) -> "RecallMemoryToolHook":
        """返回捕获 recall_memory 结果的 post-tool 钩子。"""
        return RecallMemoryToolHook(self)

    def _read_records(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        records: list[dict[str, Any]] = []
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:  # noqa: BLE001
            logger.warning("[observe] 检索记录读取失败: %s", exc)
            return []
        for line in lines:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                # 单行损坏只跳过该行:观测数据不该因为一行坏掉而整个面板打不开。
                continue
            if isinstance(value, dict):
                records.append(value)
        return records


class RecallMemoryToolHook:
    """post_tool_use 钩子:把模型主动查记忆的结果记进回放。

    只观测,永不改判决——`run` 恒返回 allow,且整段吞异常:观测坏掉不该让工具调用失败。
    """

    name = "recall_inspector"
    event = "post_tool_use"

    def __init__(self, inspector: RecallInspector) -> None:
        self._inspector = inspector

    def matches(self, ctx: Any) -> bool:
        return getattr(ctx.request, "tool_name", "") == "recall_memory"

    async def run(self, ctx: Any) -> Any:
        from agent.tool_hooks import HookOutcome

        try:
            request = ctx.request
            self._inspector.record_recall_memory(
                session_key=getattr(request, "session_key", ""),
                channel=getattr(request, "channel", ""),
                chat_id=getattr(request, "chat_id", ""),
                arguments=dict(ctx.current_arguments or {}),
                result_text=str(ctx.result or ""),
            )
        except Exception:  # noqa: BLE001 - 观测失败不影响工具结果
            logger.warning("[observe] recall_memory 回放记录失败", exc_info=True)
        return HookOutcome()
