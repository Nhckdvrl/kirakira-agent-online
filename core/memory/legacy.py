"""Markdown-backed memory runtime and searchable memory tools."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from agent.retrieval.default_pipeline import (
    LEXICAL_RRF_WEIGHT,
    RetrievalRequest,
    RetrievalResult,
    RetrievalTrace,
    hotness_boost,
    plan_injection,
    rrf_fuse,
)
from session.manager import Session, SessionManager
from core.memory.embeddings import EmbeddingClient

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _tokenize(text: str) -> set[str]:
    lowered = text.lower()
    ascii_words = set(re.findall(r"[a-z0-9_\-]{2,}", lowered))
    cjk_runs = re.findall(r"[\u4e00-\u9fff]+", lowered)
    cjk = {
        run[index : index + 2]
        for run in cjk_runs
        for index in range(max(1, len(run) - 1))
        if run[index : index + 2]
    }
    return ascii_words | cjk


def _normalize_content(text: str) -> str:
    return " ".join(text.lower().split())


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(".%s.%d.%s.tmp" % (path.name, os.getpid(), uuid4().hex))
    try:
        temp.write_text(content, encoding="utf-8")
        os.replace(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


@dataclass
class MemoryRecord:
    id: str
    content: str
    created_at: str = field(default_factory=_now)
    source_ref: str = ""
    status: str = "active"
    memory_type: str = "requested_memory"
    reinforcement: int = 1
    updated_at: str = field(default_factory=_now)
    embedding: List[float] | None = None

    def to_json(self) -> Dict[str, object]:
        return {
            "id": self.id,
            "content": self.content,
            "created_at": self.created_at,
            "source_ref": self.source_ref,
            "status": self.status,
            "memory_type": self.memory_type,
            "reinforcement": self.reinforcement,
            "updated_at": self.updated_at,
            "embedding": self.embedding,
        }

    def to_public_json(self) -> Dict[str, object]:
        payload = self.to_json()
        payload.pop("embedding", None)
        return payload


class MarkdownMemoryStore:
    def __init__(self, workspace: Path) -> None:
        self.root = workspace / "memory"
        self.root.mkdir(parents=True, exist_ok=True)
        self.memory_path = self.root / "MEMORY.md"
        self.self_path = self.root / "SELF.md"
        self.recent_path = self.root / "RECENT_CONTEXT.md"
        self.history_path = self.root / "HISTORY.md"
        self.pending_path = self.root / "PENDING.md"
        for path, title in (
            (self.memory_path, "# Long-Term Memory\n"),
            (self.self_path, "# Self Model\n"),
            (self.recent_path, "# Recent Context\n"),
            (self.history_path, "# History\n"),
            (self.pending_path, "# Pending Memory\n"),
        ):
            if not path.exists():
                path.write_text(title, encoding="utf-8")

    def read_long_term(self) -> str:
        return self.memory_path.read_text(encoding="utf-8")

    def read_self(self) -> str:
        return self.self_path.read_text(encoding="utf-8")

    def read_recent_context(self) -> str:
        return self.recent_path.read_text(encoding="utf-8")

    def append_recent(self, line: str) -> None:
        text = self.read_recent_context().rstrip()
        updated = text + "\n- %s\n" % line.strip()
        lines = updated.splitlines()
        if len(lines) > 80:
            lines = [lines[0]] + lines[-79:]
        _atomic_write(self.recent_path, "\n".join(lines) + "\n")

    def append_memory(self, record: MemoryRecord) -> None:
        self.sync_memory_records([record])

    def sync_memory_records(self, records: List[MemoryRecord]) -> None:
        start = "<!-- kirakira:managed-memory:start -->"
        end = "<!-- kirakira:managed-memory:end -->"
        existing = self.read_long_term().rstrip()
        pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.S)
        existing = pattern.sub("", existing).rstrip()
        # Migrate lines emitted by the early runtime before managed blocks existed.
        existing = re.sub(r"(?m)^- \[mem_\d+\](?: source=\S+)? .*\n?", "", existing).rstrip()
        lines = [start]
        for record in records:
            if record.status != "active":
                continue
            source = " source=%s" % record.source_ref if record.source_ref else ""
            lines.append(
                "- [%s type=%s reinforced=%d]%s %s"
                % (
                    record.id,
                    record.memory_type,
                    record.reinforcement,
                    source,
                    record.content.strip(),
                )
            )
        lines.append(end)
        _atomic_write(self.memory_path, existing + "\n\n" + "\n".join(lines) + "\n")

    def append_history(self, source_ref: str, summary: str) -> None:
        marker = "<!-- turn:%s -->" % source_ref
        existing = self.history_path.read_text(encoding="utf-8")
        if marker in existing:
            return
        timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M")
        with self.history_path.open("a", encoding="utf-8") as handle:
            handle.write("%s\n[%s] %s\n" % (marker, timestamp, summary.strip()))


class MemoryRuntime:
    def __init__(
        self,
        workspace: Path,
        session_manager: SessionManager | None = None,
        *,
        engine: str = "auto",
        shared_store: Any = None,
        event_bus: Any = None,
    ) -> None:
        self.workspace = workspace
        self.event_bus = event_bus
        self.store = MarkdownMemoryStore(workspace)
        self.session_manager = session_manager
        self.items_path = self.store.root / "items.json"
        self.owner_path = self.store.root / "structured-owner.json"
        self._records: List[MemoryRecord] = []
        self._record_lock = threading.RLock()
        self._tasks: Dict[str, asyncio.Task[None]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self.embedding_client: EmbeddingClient | None = None
        self.engine = self._resolve_engine(engine)
        # 引擎承重时由它拥有 coremem.db,这里共享同一连接,避免同库双开导致锁竞争。
        self._owns_store2 = shared_store is None
        self.store2 = shared_store
        if self.store2 is None:
            try:
                from memory2.store import MemoryStore2

                self.store2 = MemoryStore2(str(self.store.root / "coremem.db"))
            except Exception as exc:  # pragma: no cover - rollback compatibility
                logger.warning("structured store unavailable: %s", exc)
        self._load()
        if self.engine == "legacy":
            self._reconcile_store2_forgotten()
        if self.session_manager is not None:
            self.session_manager.on_delete(self._forget_session_memories)

    def _mirror_to_store2(self, record: "MemoryRecord") -> None:
        """把一条记忆镜像写入 memory2 store（upsert_item 自带去重/强化）。"""

        if self.store2 is None:
            return
        try:
            self.store2.upsert_item(
                memory_type=record.memory_type,
                summary=record.content,
                embedding=None,
                source_ref=record.source_ref or None,
            )
        except Exception:  # noqa: BLE001 - 镜像失败只记日志，不影响主记忆
            logging.getLogger(__name__).exception("memory2 mirror write failed")

    def _forget_in_store2(self, memory_type: str, content: str) -> None:
        """在 memory2 store 里退休与该 (type, content) 对应的活跃项，保持两边一致。"""

        if self.store2 is None:
            return
        try:
            items, _total = self.store2.list_items_for_dashboard(
                memory_type=memory_type, status="active", page_size=200
            )
            ids = [
                str(item["id"])
                for item in items
                if str(item.get("summary") or "") == content and item.get("id")
            ]
            if ids:
                self.store2.mark_superseded_batch(ids)
        except Exception:  # noqa: BLE001 - 传播失败只记日志
            logging.getLogger(__name__).exception("memory2 forget propagation failed")

    def _reconcile_store2_forgotten(self) -> None:
        """启动时把已 forgotten 的记录同步到 memory2 store，修复历史不一致。"""

        if self.store2 is None:
            return
        with self._record_lock:
            forgotten = [
                (record.memory_type, record.content)
                for record in self._records
                if record.status == "forgotten"
            ]
        for memory_type, content in forgotten:
            self._forget_in_store2(memory_type, content)

    def configure_embeddings(
        self, *, base_url: str, api_key: str, model: str
    ) -> None:
        if base_url.strip() and model.strip():
            self.embedding_client = EmbeddingClient(
                base_url=base_url,
                api_key=api_key,
                model=model,
            )

    def _resolve_engine(self, requested: str) -> str:
        explicit = os.getenv("KIRAKIRA_MEMORY_ENGINE", requested).strip().lower()
        if explicit in {"legacy", "coremem"}:
            return explicit
        if explicit not in {"", "auto"}:
            raise ValueError("memory engine 必须是 auto/legacy/coremem")
        if self.owner_path.exists():
            payload = json.loads(self.owner_path.read_text(encoding="utf-8"))
            owner = str(payload.get("owner") or "").strip().lower()
            if owner in {"legacy", "coremem"}:
                return owner
            raise RuntimeError("structured-owner.json owner 无效")
        # Pre-M1 workspaces continue on the old owner until an explicit staged
        # migration publishes the marker. A new workspace starts legacy too so
        # rollback remains deterministic during M1.
        return "legacy"

    @staticmethod
    def _canonical_memory_type(memory_type: str) -> tuple[str, dict[str, object]]:
        raw = memory_type.strip() or "requested_memory"
        if raw in {"identity", "fact", "requested_memory"}:
            return "profile", {"legacy_memory_type": raw}
        if raw not in {"procedure", "preference", "event", "profile"}:
            return "profile", {"legacy_memory_type": raw}
        return raw, {}

    def memorize(
        self,
        content: str,
        source_ref: str = "",
        memory_type: str = "requested_memory",
    ) -> MemoryRecord:
        if self.engine == "coremem":
            return self._memorize_memory2(content, source_ref, memory_type)
        with self._record_lock:
            content = content.strip()
            if not content:
                raise ValueError("memory content is empty")
            normalized = _normalize_content(content)
            for record in self._records:
                if record.status == "active" and _normalize_content(record.content) == normalized:
                    if source_ref and source_ref == record.source_ref:
                        return record
                    record.reinforcement += 1
                    record.updated_at = _now()
                    if source_ref:
                        record.source_ref = source_ref
                    self._save()
                    self._mirror_to_store2(record)
                    return record
            record = MemoryRecord(
                id=self._next_id(),
                content=content,
                source_ref=source_ref,
                memory_type=memory_type.strip() or "requested_memory",
                embedding=self._embed_for_store(content),
            )
            self._records.append(record)
            self._save()
            self._mirror_to_store2(record)
            return record

    def _memorize_memory2(
        self,
        content: str,
        source_ref: str,
        memory_type: str,
    ) -> MemoryRecord:
        if self.store2 is None:
            raise RuntimeError("Memory2 store 未初始化")
        summary = content.strip()
        if not summary:
            raise ValueError("memory content is empty")
        canonical_type, extra = self._canonical_memory_type(memory_type)
        if canonical_type == "procedure":
            from memory2.rule_schema import build_procedure_rule_schema

            extra["rule_schema"] = build_procedure_rule_schema(summary)
        embedding = self._embed_for_store(summary)
        result = self.store2.upsert_item(
            memory_type=canonical_type,
            summary=summary,
            embedding=embedding,
            source_ref=source_ref or None,
            extra=extra or None,
        )
        item_id = result.split(":", 1)[1]
        if source_ref:
            self.store2.update_item_for_dashboard(item_id, source_ref=source_ref)
        self._load()
        return next(record for record in self._records if record.id == item_id)

    def candidates(
        self,
        memory_types: List[str] | None = None,
        since: str = "",
        until: str = "",
    ) -> List[MemoryRecord]:
        """按 type/时间过滤出候选集合；排序交给各 lane。"""

        if self.engine == "coremem":
            self._load()
        allowed_types = {
            self._canonical_memory_type(item)[0]
            for item in (memory_types or [])
            if item
        }
        since_dt = self._parse_optional_time(since)
        until_dt = self._parse_optional_time(until)
        with self._record_lock:
            records = list(self._records)
        selected: List[MemoryRecord] = []
        for record in records:
            if record.status != "active":
                continue
            if allowed_types and record.memory_type not in allowed_types:
                continue
            created = self._parse_optional_time(record.created_at)
            if since_dt and created and created < since_dt:
                continue
            if until_dt and created and created > until_dt:
                continue
            selected.append(record)
        return selected

    def lexical_lane(
        self, query: str, records: List[MemoryRecord]
    ) -> List[MemoryRecord]:
        """词法 lane：擅长变量名、命令、路径、错误码这类精确实体。"""

        q_tokens = _tokenize(query)
        needle = query.lower().strip()
        scored: List[tuple[float, str, MemoryRecord]] = []
        for record in records:
            tokens = _tokenize(record.content)
            overlap = len(q_tokens & tokens)
            score = overlap / max(1.0, math.sqrt(len(q_tokens) * max(1, len(tokens))))
            # 整串命中是很强的信号，但只在本 lane 内部抬名次，不会跨 lane 污染分数。
            if needle and needle in record.content.lower():
                score += 1.0
            if score <= 0:
                continue
            scored.append((score, record.created_at, record))
        scored.sort(key=lambda item: (-item[0], item[1]), reverse=False)
        scored.sort(key=lambda item: item[0], reverse=True)
        return [record for _, _, record in scored]

    def vector_lane(
        self, query: str, records: List[MemoryRecord], *, threshold: float = 0.25
    ) -> List[MemoryRecord]:
        """语义 lane：擅长口语化表达和同义改写。向量不可用时返回空，退化为纯词法。"""

        query_embedding = self._embed_for_query(query) if query.strip() else None
        if query_embedding is None:
            return []
        scored: List[tuple[float, str, MemoryRecord]] = []
        for record in records:
            semantic = self._cosine(query_embedding, record.embedding)
            if semantic is None or semantic < threshold:
                continue
            scored.append((semantic, record.created_at, record))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [record for _, _, record in scored]

    def recall(
        self,
        query: str,
        limit: int = 5,
        memory_types: List[str] | None = None,
        since: str = "",
        until: str = "",
    ) -> List[MemoryRecord]:
        """多路召回 + RRF 融合。

        不再用 `semantic * 0.75 + lexical * 0.25`：那是把尺度不可比的两个原始分数直接
        相加。RRF 只看各 lane 内部的名次，因此 lane 之间不需要可比。
        """

        return self._recall_with_trace(query, limit, memory_types, since, until)[0]

    def _recall_with_trace(
        self,
        query: str,
        limit: int,
        memory_types: List[str] | None,
        since: str,
        until: str,
    ) -> tuple[List[MemoryRecord], RetrievalTrace]:
        """召回并同时产出 trace。lane 只跑一次——vector lane 会打 embedding 接口，
        为了记 trace 再跑一遍等于每轮多花一次网络往返。"""

        trace = RetrievalTrace(used_vector=self.embedding_client is not None)
        records = self.candidates(memory_types, since, until)
        if not records:
            return [], trace
        if not query.strip():
            # 无 query 时没有"相关性"可言，按时间倒序给最近的。
            selected = sorted(records, key=lambda r: r.created_at, reverse=True)[
                : max(1, limit)
            ]
            trace.fused = len(selected)
            return selected, trace

        lexical = self.lexical_lane(query, records)
        vector = self.vector_lane(query, records)
        trace.lanes = {"lexical": len(lexical), "vector": len(vector)}
        fused = rrf_fuse(
            [("vector", 1.0, vector), ("lexical", LEXICAL_RRF_WEIGHT, lexical)]
        )
        by_id = {record.id: record for record in records}
        now = datetime.now().astimezone()
        boosted: List[tuple[float, str, MemoryRecord]] = []
        for record_id, score in fused:
            record = by_id.get(record_id)
            if record is None:
                continue
            # 强化次数的加成随时间半衰，陈年旧记忆不会永远压住新记忆。
            score *= hotness_boost(
                record.reinforcement,
                self._parse_optional_time(record.updated_at),
                now,
            )
            boosted.append((score, record.created_at, record))
        boosted.sort(key=lambda item: (-item[0], item[1]))
        selected = [record for _, _, record in boosted[: max(1, limit)]]
        trace.fused = len(selected)
        return selected, trace

    def forget(self, ids: List[str]) -> List[str]:
        if self.engine == "coremem":
            if self.store2 is None:
                raise RuntimeError("Memory2 store 未初始化")
            active = {
                str(item["id"])
                for item in self.store2.get_items_by_ids(ids)
                if item.get("status") == "active"
            }
            affected = [item_id for item_id in ids if item_id in active]
            self.store2.mark_superseded_batch(affected)
            self._load()
            return affected
        with self._record_lock:
            forgotten: List[str] = []
            forgotten_targets: List[tuple[str, str]] = []
            wanted = set(ids)
            for record in self._records:
                if record.id in wanted and record.status == "active":
                    record.status = "forgotten"
                    forgotten.append(record.id)
                    forgotten_targets.append((record.memory_type, record.content))
            if forgotten:
                self._save()
        # 锁外传播到 memory2，避免持锁做 IO。
        for memory_type, content in forgotten_targets:
            self._forget_in_store2(memory_type, content)
        return forgotten

    def list_records(self, *, include_forgotten: bool = False) -> List[Dict[str, object]]:
        if self.engine == "coremem":
            self._load()
        with self._record_lock:
            records = [
                record
                for record in self._records
                if include_forgotten or record.status == "active"
            ]
        records.sort(key=lambda item: item.updated_at, reverse=True)
        return [record.to_public_json() for record in records]

    def update_record(
        self,
        memory_id: str,
        *,
        content: str | None = None,
        memory_type: str | None = None,
    ) -> bool:
        if self.engine == "coremem":
            if self.store2 is None:
                raise RuntimeError("Memory2 store 未初始化")
            canonical_type = None
            if memory_type is not None:
                canonical_type = self._canonical_memory_type(memory_type)[0]
            embedding = None
            replace_embedding = False
            if content is not None:
                value = content.strip()
                if not value:
                    raise ValueError("memory content is empty")
                embedding = self._embed_for_store(value)
                replace_embedding = True
            updated = self.store2.replace_item_content(
                memory_id,
                summary=content,
                memory_type=canonical_type,
                embedding=embedding,
                replace_embedding=replace_embedding,
            )
            self._load()
            return updated is not None
        with self._record_lock:
            for record in self._records:
                if record.id != memory_id:
                    continue
                if content is not None:
                    value = content.strip()
                    if not value:
                        raise ValueError("memory content is empty")
                    record.content = value
                    record.embedding = self._embed_for_store(value)
                if memory_type is not None:
                    record.memory_type = memory_type.strip() or record.memory_type
                record.updated_at = _now()
                self._save()
                return True
            return False

    def build_retrieval_block(self, query: str, limit: int = 5) -> str:
        return self.retrieve(RetrievalRequest(query=query, limit=limit)).block

    def retrieve(self, request: RetrievalRequest) -> RetrievalResult:
        """默认检索管线：多路召回 → RRF 融合 → 热度加权 → 注入预算。

        实现 `MemoryRetrievalPipeline` 协议，因此可以整体替换成别的策略。
        """

        records, trace = self._recall_with_trace(
            request.query,
            request.limit,
            list(request.memory_types) or None,
            request.since,
            request.until,
        )
        block, injected, truncated = plan_injection(records)
        trace.injected = injected
        trace.truncated = truncated
        return RetrievalResult(block=block, records=records, trace=trace)




    async def shutdown(self, timeout: float = 30.0) -> None:
        tasks = [task for task in self._tasks.values() if not task.done()]
        if tasks:
            _done, pending = await asyncio.wait(tasks, timeout=max(0.1, timeout))
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        # 共享连接由引擎负责关闭,这里只关自己开的那个,避免提前关掉引擎在用的库。
        if self.store2 is not None and self._owns_store2:
            self.store2.close()


    def _publish_consolidation(
        self,
        session: Session,
        source_ref: str,
        history_entries: List[str],
        selected: List[JsonDict],
    ) -> None:
        """发布 ConsolidationCommitted。

        没有 event_bus 时是空操作;发布失败只记日志——consolidation 本身已经提交,
        不能因为下游提取失败而回滚已经写好的归档。
        """
        if self.event_bus is None or not history_entries:
            return
        try:
            from core.memory.events import ConsolidationCommitted

            conversation = "\n".join(
                "%s: %s" % (item.get("role") or "", str(item.get("content") or "")[:500])
                for item in selected
            )
            self.event_bus.enqueue(
                ConsolidationCommitted(
                    history_entry_payloads=[(entry, 0) for entry in history_entries],
                    source_ref=source_ref,
                    scope_channel=str(session.metadata.get("channel") or ""),
                    scope_chat_id=str(session.metadata.get("chat_id") or ""),
                    conversation=conversation,
                    user_id=str(session.metadata.get("principal_id") or ""),
                )
            )
        except Exception:  # noqa: BLE001 - 归档已提交,下游失败不回滚
            logger.warning("consolidation event publish failed", exc_info=True)



    def search_messages(self, query: str, limit: int = 10) -> List[Dict[str, str]]:
        if self.session_manager is None:
            return []
        return self.session_manager.search_messages(query, limit=limit)  # type: ignore[return-value]

    def fetch_messages(self, source_ref: str, context: int = 2) -> List[Dict[str, str]]:
        if self.session_manager is None:
            return []
        return self.session_manager.fetch_messages(source_ref, context=context)  # type: ignore[return-value]


    def _load(self) -> None:
        if self.engine == "coremem":
            self._load_from_store2()
            return
        if not self.items_path.exists():
            self._records = []
            return
        data = json.loads(self.items_path.read_text(encoding="utf-8"))
        self._records = [
            MemoryRecord(
                id=str(item.get("id") or ""),
                content=str(item.get("content") or ""),
                created_at=str(item.get("created_at") or _now()),
                source_ref=str(item.get("source_ref") or ""),
                status=str(item.get("status") or "active"),
                memory_type=str(item.get("memory_type") or "requested_memory"),
                reinforcement=max(1, int(item.get("reinforcement") or 1)),
                updated_at=str(item.get("updated_at") or item.get("created_at") or _now()),
                embedding=[float(value) for value in item.get("embedding", [])]
                if isinstance(item.get("embedding"), list) and item.get("embedding")
                else None,
            )
            for item in data
            if item.get("id") and item.get("content")
        ]

    def _load_from_store2(self) -> None:
        if self.store2 is None:
            raise RuntimeError("Memory2 store 未初始化")
        records: List[MemoryRecord] = []
        page = 1
        while True:
            items, total = self.store2.list_items_for_dashboard(
                page=page,
                page_size=200,
                sort_by="created_at",
                sort_order="asc",
            )
            for item in items:
                detail = self.store2.get_item_for_dashboard(
                    str(item["id"]), include_embedding=True
                )
                if detail is None:
                    continue
                records.append(
                    MemoryRecord(
                        id=str(detail["id"]),
                        content=str(detail["summary"]),
                        created_at=str(detail["created_at"]),
                        source_ref=str(detail.get("source_ref") or ""),
                        status=str(detail["status"]),
                        memory_type=str(detail["memory_type"]),
                        reinforcement=max(1, int(detail.get("reinforcement") or 1)),
                        updated_at=str(detail["updated_at"]),
                        embedding=detail.get("embedding")
                        if isinstance(detail.get("embedding"), list)
                        else None,
                    )
                )
            if page * 200 >= total:
                break
            page += 1
        self._records = records

    def _save(self) -> None:
        if self.engine == "coremem":
            raise RuntimeError("Memory2 模式禁止写 items.json")
        _atomic_write(
            self.items_path,
            json.dumps([r.to_json() for r in self._records], ensure_ascii=False, indent=2),
        )
        self.store.sync_memory_records(self._records)

    def _next_id(self) -> str:
        highest = 0
        for record in self._records:
            match = re.fullmatch(r"mem_(\d+)", record.id)
            if match:
                highest = max(highest, int(match.group(1)))
        return "mem_%04d" % (highest + 1)


    def _embed_for_query(self, text: str) -> List[float] | None:
        """检索侧可以降级：拿不到向量就退回词法召回，本轮仍然有答案。"""

        if self.embedding_client is None or not text.strip():
            return None
        try:
            return self.embedding_client.embed(text)
        except Exception:
            logger.exception("embedding failed; falling back to lexical recall")
            return None

    def _embed_for_store(self, text: str) -> List[float] | None:
        """写入侧不能降级：配置了 embedding 却静默存入无向量记录，会让这条记忆此后
        永远无法被语义召回，且索引里一部分有向量一部分没有，是不可见的数据损坏。"""

        if self.embedding_client is None or not text.strip():
            return None
        try:
            return self.embedding_client.embed(text)
        except Exception as exc:
            raise RuntimeError(
                "embedding service failed while storing memory; refusing to write a "
                "record that could never be recalled semantically"
            ) from exc

    @staticmethod
    def _cosine(
        first: List[float] | None, second: List[float] | None
    ) -> float | None:
        if not first or not second or len(first) != len(second):
            return None
        dot = sum(a * b for a, b in zip(first, second))
        first_norm = math.sqrt(sum(value * value for value in first))
        second_norm = math.sqrt(sum(value * value for value in second))
        if first_norm <= 0 or second_norm <= 0:
            return None
        return dot / (first_norm * second_norm)

    @staticmethod
    def _parse_optional_time(value: str) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.astimezone()


    def _forget_session_memories(self, session_key: str) -> None:
        prefix = session_key + ":"
        if self.engine == "coremem":
            if self.store2 is None:
                raise RuntimeError("Memory2 store 未初始化")
            items, _ = self.store2.list_items_for_dashboard(
                status="active", source_ref=prefix, page_size=200
            )
            ids = [
                str(item["id"])
                for item in items
                if str(item.get("source_ref") or "").startswith(prefix)
            ]
            self.store2.mark_superseded_batch(ids)
            self._load()
            return
        with self._record_lock:
            changed = False
            for record in self._records:
                if record.status == "active" and record.source_ref.startswith(prefix):
                    record.status = "forgotten"
                    record.updated_at = _now()
                    changed = True
            if changed:
                self._save()
