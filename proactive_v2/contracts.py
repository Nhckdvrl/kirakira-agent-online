"""三通道事件契约与归一化。

参考 akashic 的 `proactive_v2/contracts.py`，MVP 精简为渲染 prompt 所需的最小字段。

三种通道语义：
- ``alert``   高优先级告警，优先发送并由模型自然化表达
- ``content`` 内容候选，经 LLM 兴趣判断后决定是否推送
- ``context`` 背景状态，只辅助判断，不单独触发推送
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict

VALID_CHANNELS = ("alert", "content", "context")


def _text(value: Any) -> str:
    return str(value or "").strip()


def canonical_item_id(source_id: str, event: Dict[str, Any]) -> str:
    """稳定事件身份：``<source_id>:<event_id>``，用于跨轮去重与 ACK。"""
    event_id = _text(event.get("event_id") or event.get("id")) or "?"
    return "%s:%s" % (source_id or "?", event_id)


@dataclass(slots=True)
class AlertContract:
    item_id: str
    title: str
    content: str
    severity: str
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_prompt_line(self) -> str:
        parts = [f"id={self.item_id}", f"title={self.title}"]
        if self.severity:
            parts.append(f"severity={self.severity}")
        if self.content:
            parts.append(f"内容：{self.content}")
        return "  " + "\n       ".join(parts)


@dataclass(slots=True)
class ContentContract:
    item_id: str
    title: str
    source: str
    url: str
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_prompt_line(self, index: int) -> str:
        url_part = f"\n       url={self.url}" if self.url else ""
        return (
            f"  [{index}] id={self.item_id}\n"
            f"       title={self.title}\n"
            f"       source={self.source}{url_part}"
        )


@dataclass(slots=True)
class ContextContract:
    source: str
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_prompt_line(self) -> str:
        payload = dict(self.raw)
        payload.pop("kind", None)
        return "  " + json.dumps(payload, ensure_ascii=False, sort_keys=True)


def normalize_alert(event: Dict[str, Any]) -> AlertContract:
    return AlertContract(
        item_id=_text(event.get("item_id")) or canonical_item_id(_text(event.get("_source")), event),
        title=_text(event.get("title")),
        content=_text(event.get("content") or event.get("body")),
        severity=_text(event.get("severity")),
        raw=event,
    )


def normalize_content(event: Dict[str, Any]) -> ContentContract:
    return ContentContract(
        item_id=_text(event.get("item_id")) or canonical_item_id(_text(event.get("_source")), event),
        title=_text(event.get("title")),
        source=_text(event.get("source") or event.get("source_name") or event.get("_source")),
        url=_text(event.get("url")),
        raw=event,
    )


def normalize_context(event: Dict[str, Any]) -> ContextContract:
    return ContextContract(
        source=_text(event.get("_source") or event.get("source")),
        raw=event,
    )


# ── 事件排序（对齐 reference 的 rank_events）──────────────────────

_SEVERITY_ORDER = {"critical": 3, "high": 3, "medium": 2, "low": 1, "": 0}


def _event_time(event: Dict[str, Any]) -> str:
    """取事件时间用于排序：优先 published_at，其次 first_seen_at。ISO 串可字典序比较。"""
    return _text(event.get("published_at") or event.get("first_seen_at"))


def rank_content(events: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """内容候选按新近度排序：新的优先（reference 用 recency + 兴趣打分，MVP 先按时间）。"""
    return sorted(events, key=_event_time, reverse=True)


def rank_alerts(events: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """告警按严重度优先、其次新近度排序。"""
    return sorted(
        events,
        key=lambda e: (
            _SEVERITY_ORDER.get(_text(e.get("severity")).lower(), 0),
            _event_time(e),
        ),
        reverse=True,
    )
