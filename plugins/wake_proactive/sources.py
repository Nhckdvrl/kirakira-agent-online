"""可插拔的主动数据源。

参考 akashic 用 MCP 插件声明 ``ProactiveSourceSpec``（fetch_tool + ack_tool），
核心 runtime 每个 tick 并发调用。kirakira 暂无 proactive-source 插件运行时，
所以 MVP 定义一个进程内协议 + 注册表，先内置一个从文件读事件的源。

日后接入 MCP：实现同一个 ``ProactiveSource`` 协议（``fetch`` 调 fetch_tool，
``ack`` 调 ack_tool）即可无缝替换，链路其余部分不感知数据来自哪里。
"""

from __future__ import annotations

import asyncio
import json
import inspect
import logging
from pathlib import Path
from typing import Any, Dict, List, Protocol, Sequence, runtime_checkable

from proactive_v2.contracts import VALID_CHANNELS, canonical_item_id

logger = logging.getLogger(__name__)


@runtime_checkable
class ProactiveSource(Protocol):
    """一个主动数据源。稳定身份是 ``id``；只理解自己声明的 ``channels``。"""

    id: str
    channels: Sequence[str]

    async def fetch(self) -> List[Dict[str, Any]]:
        """返回本源当前快照的事件列表。每个事件需带 ``kind`` 与 ``event_id``。"""
        ...

    async def ack(
        self, event_ids: Sequence[str], feedback: str | None = None
    ) -> None:
        """确认已成功投递的原始事件；只按 event_id 确认，不用标题/URL 代替。"""
        ...


class SourceRegistry:
    """聚合所有已注册数据源，按通道分组返回快照。"""

    def __init__(self) -> None:
        self._sources: Dict[str, ProactiveSource] = {}

    def add(self, source: ProactiveSource) -> None:
        if source.id in self._sources:
            raise ValueError("duplicate proactive source id: %s" % source.id)
        self._sources[source.id] = source

    @property
    def sources(self) -> List[ProactiveSource]:
        return list(self._sources.values())

    async def _safe_fetch(self, source: ProactiveSource) -> List[Dict[str, Any]]:
        """拉取单个源；失败不外抛，记日志返回空，保证部分可用。"""
        try:
            return await source.fetch()
        except Exception:
            logger.exception("[proactive.source] fetch 失败 source=%s", source.id)
            return []

    async def fetch_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """并发拉取所有源，按 alert/content/context 分组。

        真正并发（asyncio.gather）：MCP 源是 I/O 密集，顺序拉会把 tick 拖长。
        单个源失败不影响其他源（记日志跳过），符合"部分可用"语义。
        """
        sources = list(self._sources.values())
        all_events = await asyncio.gather(
            *(self._safe_fetch(source) for source in sources)
        )
        grouped: Dict[str, List[Dict[str, Any]]] = {k: [] for k in VALID_CHANNELS}
        for source, events in zip(sources, all_events):
            for event in events:
                kind = str(event.get("kind") or "").strip()
                if kind not in grouped:
                    logger.warning(
                        "[proactive.source] 未知 kind=%r source=%s 已忽略",
                        kind,
                        source.id,
                    )
                    continue
                enriched = dict(event)
                enriched["_source"] = source.id
                enriched.setdefault("item_id", canonical_item_id(source.id, enriched))
                grouped[kind].append(enriched)
        return grouped

    async def ack(
        self,
        source_id: str,
        event_ids: Sequence[str],
        feedback: str | None = None,
    ) -> bool:
        """回源确认并返回是否成功，供 runtime 决定是否删除 pending ACK。"""
        source = self._sources.get(source_id)
        if source is None:
            logger.warning("[proactive.source] ack 未知源 source=%s", source_id)
            return False
        try:
            parameters = inspect.signature(source.ack).parameters
            if "feedback" in parameters or any(
                item.kind is inspect.Parameter.VAR_KEYWORD
                for item in parameters.values()
            ):
                await source.ack(event_ids, feedback=feedback)
            else:
                await source.ack(event_ids)
            return True
        except Exception:
            logger.exception("[proactive.source] ack 失败 source=%s", source_id)
            return False


class FileInboxSource:
    """从 ``<workspace>/proactive/inbox/<id>.jsonl`` 读事件的内置源。

    每行一个 JSON 事件对象。ACK 后把 event_id 写入同目录的 ``<id>.acked``，
    下次 fetch 自动过滤，避免重复投递——用最朴素的方式演示完整 fetch/ack 闭环。
    """

    def __init__(self, source_id: str, inbox_dir: Path, channels: Sequence[str]) -> None:
        self.id = source_id
        self.channels = tuple(channels)
        self._path = inbox_dir / ("%s.jsonl" % source_id)
        self._acked_path = inbox_dir / ("%s.acked" % source_id)

    def _load_acked(self) -> set[str]:
        if not self._acked_path.exists():
            return set()
        return {
            line.strip()
            for line in self._acked_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }

    async def fetch(self) -> List[Dict[str, Any]]:
        if not self._path.exists():
            return []
        acked = self._load_acked()
        events: List[Dict[str, Any]] = []
        for raw_line in self._path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("[proactive.source] 跳过非法 JSON 行 source=%s", self.id)
                continue
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("event_id") or event.get("id") or "").strip()
            if event_id and event_id in acked:
                continue
            events.append(event)
        return events

    async def ack(
        self, event_ids: Sequence[str], feedback: str | None = None
    ) -> None:
        wanted = [str(eid).strip() for eid in event_ids if str(eid).strip()]
        if not wanted:
            return
        self._acked_path.parent.mkdir(parents=True, exist_ok=True)
        existing = self._load_acked()
        new_ids = [eid for eid in wanted if eid not in existing]
        if new_ids:
            with self._acked_path.open("a", encoding="utf-8") as handle:
                for eid in new_ids:
                    handle.write(eid + "\n")
        if feedback:
            feedback_path = self._acked_path.with_suffix(".feedback.jsonl")
            with feedback_path.open("a", encoding="utf-8") as handle:
                for event_id in wanted:
                    handle.write(
                        json.dumps(
                            {"event_id": event_id, "feedback": feedback},
                            ensure_ascii=False,
                        )
                        + "\n"
                    )


def build_file_inbox_registry(workspace: Path) -> SourceRegistry:
    """扫描 ``<workspace>/proactive/inbox/*.jsonl``，为每个文件建一个源。

    源默认声明 alert+content+context 三通道；实际 kind 以事件里的 ``kind`` 为准。
    目录不存在时会创建并留一个 README，方便用户/演示往里投事件。
    """
    inbox = workspace / "proactive" / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    readme = inbox / "README.md"
    if not readme.exists():
        readme.write_text(_INBOX_README, encoding="utf-8")
    registry = SourceRegistry()
    for path in sorted(inbox.glob("*.jsonl")):
        if path.name.endswith(".feedback.jsonl"):
            continue
        registry.add(FileInboxSource(path.stem, inbox, VALID_CHANNELS))
    return registry


_INBOX_README = """# Proactive Inbox（内置文件数据源）

往这个目录放 `<source>.jsonl`，每行一个 JSON 事件，主动链路每个 tick 会读取。
这是 MVP 的内置数据源；生产环境可换成 MCP 插件源（实现同一个 ProactiveSource 协议）。

事件字段：
- `kind`：`alert` | `content` | `context`
- `event_id`：源内稳定 ID（用于去重和 ACK）
- alert：`title`、`content`、`severity`
- content：`title`、`source`、`url`
- context：任意背景字段（如 `available`、`summary`）

示例（`demo.jsonl`）：

    {"kind": "alert", "event_id": "a1", "title": "会议 10 分钟后开始", "content": "项目周会", "severity": "medium"}
    {"kind": "content", "event_id": "c1", "title": "Rust 1.90 发布", "source": "hn", "url": "https://example.com/rust"}
    {"kind": "context", "event_id": "x1", "available": true, "summary": "用户当前更可能醒着"}

投递成功后，event_id 会被写入 `<source>.acked`，下次自动跳过。
"""
