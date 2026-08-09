"""Fail-closed, user-scoped persistence adapter for the Memory v2 algorithms."""

from __future__ import annotations

import json
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import Iterator
from uuid import UUID, uuid4

from sqlalchemy import Engine, Select, bindparam, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from cloud.models import (
    MemoryConsolidationEvent,
    MemoryItem,
    MemoryReplacement,
    utc_now,
)
from memory2.store import (
    EmbeddingRow,
    MemoryHit,
    _coerce_emotional_weight,
    _content_hash,
    _cosine_similarity,
    _is_memory_time_in_range,
    _parse_memory_time,
    score_embedding_rows,
)


class UserScopedPostgresMemoryStore:
    """Synchronous Memory persistence backed by SQLAlchemy/PostgreSQL.

    Memory v2's ranking and consolidation algorithms are asynchronous around a
    synchronous persistence port.  A worker may therefore use a normal pooled
    SQLAlchemy engine.  Every operation requires an explicit task-local user
    binding and adds ``user_id`` to its predicate; an unbound call is rejected.

    The implementation is intentionally dialect-neutral for deterministic unit
    tests, while production uses a ``postgresql+psycopg`` engine.
    """

    VECTOR_DIMENSION = 1024
    VECTOR_CANDIDATE_FLOOR = 256
    VECTOR_CANDIDATE_MULTIPLIER = 32

    def __init__(self, engine: Engine, *, vector_dimension: int = 1024) -> None:
        if vector_dimension != self.VECTOR_DIMENSION:
            raise ValueError(
                f"Cloud pgvector dimension must be {self.VECTOR_DIMENSION}"
            )
        self._engine = engine
        self._vector_dimension = vector_dimension
        self._bound_user: ContextVar[UUID | None] = ContextVar(
            "cloud_memory_user", default=None
        )

    def close(self) -> None:
        self._engine.dispose()

    @contextmanager
    def bind_user(self, user_id: UUID | str) -> Iterator[None]:
        parsed = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        token = self._bound_user.set(parsed)
        try:
            yield
        finally:
            self._bound_user.reset(token)

    def _user_id(self) -> UUID:
        user_id = self._bound_user.get()
        if user_id is None:
            raise RuntimeError("Cloud memory access requires an active user binding")
        return user_id

    @staticmethod
    def _new_id() -> str:
        return uuid4().hex[:12]

    def upsert_item(
        self,
        memory_type: str,
        summary: str,
        embedding: list[float] | None,
        source_ref: str | None = None,
        extra: dict[str, object] | None = None,
        happened_at: str | None = None,
        emotional_weight: int = 0,
    ) -> str:
        user_id = self._user_id()
        content_hash = _content_hash(summary, memory_type)
        emotional_weight = _coerce_emotional_weight(emotional_weight)
        try:
            with Session(self._engine) as db, db.begin():
                existing = db.scalar(
                    select(MemoryItem)
                    .where(
                        MemoryItem.user_id == user_id,
                        MemoryItem.content_hash == content_hash,
                        MemoryItem.memory_type == memory_type,
                    )
                    .with_for_update()
                )
                if existing is not None:
                    self._reinforce_item(existing, emotional_weight)
                    return f"reinforced:{existing.id}"

                item_id = self._new_id()
                db.add(
                    item := MemoryItem(
                        id=item_id,
                        user_id=user_id,
                        memory_type=memory_type,
                        summary=summary,
                        content_hash=content_hash,
                        embedding=embedding,
                        emotional_weight=emotional_weight,
                        extra_json=dict(extra) if extra else None,
                        source_ref=source_ref,
                        happened_at=happened_at,
                    )
                )
                db.flush()
                self._sync_pgvector(db, item.id, embedding)
                return f"new:{item_id}"
        except IntegrityError:
            # Concurrent identical writes race between SELECT and INSERT.  The
            # unique user/hash/type key chooses the winner; this transaction
            # applies the exact reinforcement semantics to the winning row.
            with Session(self._engine) as db, db.begin():
                existing = db.scalar(
                    select(MemoryItem)
                    .where(
                        MemoryItem.user_id == user_id,
                        MemoryItem.content_hash == content_hash,
                        MemoryItem.memory_type == memory_type,
                    )
                    .with_for_update()
                )
                if existing is None:
                    raise
                self._reinforce_item(existing, emotional_weight)
                return f"reinforced:{existing.id}"

    def upsert_consolidation_event(
        self,
        *,
        source_ref: str,
        summary: str,
        embedding: list[float] | None,
        extra: dict[str, object] | None = None,
        happened_at: str | None = None,
        emotional_weight: int = 0,
    ) -> str:
        user_id = self._user_id()
        source = (source_ref or "").strip()
        text = (summary or "").strip()
        if not source or not text:
            return "skipped:empty"
        weight = _coerce_emotional_weight(emotional_weight)
        try:
            with Session(self._engine) as db, db.begin():
                committed = db.scalar(
                    select(MemoryConsolidationEvent).where(
                        MemoryConsolidationEvent.user_id == user_id,
                        MemoryConsolidationEvent.source_ref == source,
                    )
                )
                if committed is not None:
                    return f"skipped:{committed.item_id or source}"

                content_hash = _content_hash(text, "event")
                item = db.scalar(
                    select(MemoryItem)
                    .where(
                        MemoryItem.user_id == user_id,
                        MemoryItem.content_hash == content_hash,
                        MemoryItem.memory_type == "event",
                    )
                    .with_for_update()
                )
                if item is None:
                    item = MemoryItem(
                        id=self._new_id(),
                        user_id=user_id,
                        memory_type="event",
                        summary=text,
                        content_hash=content_hash,
                        embedding=embedding,
                        emotional_weight=weight,
                        extra_json=dict(extra) if extra else None,
                        source_ref=source,
                        happened_at=happened_at,
                    )
                    db.add(item)
                    db.flush()
                    self._sync_pgvector(db, item.id, embedding)
                    result = f"new:{item.id}"
                else:
                    self._reinforce_item(item, weight, happened_at=happened_at)
                    result = f"reinforced:{item.id}"
                db.add(
                    MemoryConsolidationEvent(
                        user_id=user_id, source_ref=source, item_id=item.id
                    )
                )
                return result
        except IntegrityError:
            # Resolve source-ref or content races after the winning transaction
            # commits, without ever crossing the user boundary.
            with Session(self._engine) as db, db.begin():
                committed = db.scalar(
                    select(MemoryConsolidationEvent).where(
                        MemoryConsolidationEvent.user_id == user_id,
                        MemoryConsolidationEvent.source_ref == source,
                    )
                )
                if committed is not None:
                    return f"skipped:{committed.item_id or source}"
                item = db.scalar(
                    select(MemoryItem)
                    .where(
                        MemoryItem.user_id == user_id,
                        MemoryItem.content_hash == _content_hash(text, "event"),
                        MemoryItem.memory_type == "event",
                    )
                    .with_for_update()
                )
                if item is None:
                    raise
                self._reinforce_item(item, weight, happened_at=happened_at)
                db.add(
                    MemoryConsolidationEvent(
                        user_id=user_id, source_ref=source, item_id=item.id
                    )
                )
                return f"reinforced:{item.id}"

    def has_consolidation_source_ref(self, source_ref: str) -> bool:
        user_id = self._user_id()
        with Session(self._engine) as db:
            return db.scalar(
                select(MemoryConsolidationEvent.id).where(
                    MemoryConsolidationEvent.user_id == user_id,
                    MemoryConsolidationEvent.source_ref == (source_ref or "").strip(),
                )
            ) is not None

    def mark_superseded_batch(self, ids: list[str]) -> None:
        if not ids:
            return
        user_id = self._user_id()
        with Session(self._engine) as db, db.begin():
            items = db.scalars(
                select(MemoryItem).where(
                    MemoryItem.user_id == user_id, MemoryItem.id.in_(ids)
                )
            )
            now = utc_now()
            for item in items:
                item.status = "superseded"
                item.updated_at = now

    def get_items_by_ids(self, ids: list[str]) -> list[dict[str, object]]:
        if not ids:
            return []
        user_id = self._user_id()
        with Session(self._engine) as db:
            items = db.scalars(
                select(MemoryItem).where(
                    MemoryItem.user_id == user_id, MemoryItem.id.in_(ids)
                )
            ).all()
        by_id = {item.id: self._item_dict(item) for item in items}
        return [by_id[item_id] for item_id in ids if item_id in by_id]

    def reinforce_items_batch(
        self, ids: list[str], emotional_weight: int = 0
    ) -> None:
        if not ids:
            return
        user_id = self._user_id()
        weight = _coerce_emotional_weight(emotional_weight)
        with Session(self._engine) as db, db.begin():
            items = db.scalars(
                select(MemoryItem).where(
                    MemoryItem.user_id == user_id, MemoryItem.id.in_(ids)
                )
            )
            now = utc_now()
            for item in items:
                item.reinforcement += 1
                item.emotional_weight = max(item.emotional_weight, weight)
                item.updated_at = now

    def record_replacements(
        self,
        *,
        old_items: list[dict[str, object]],
        new_item: dict[str, object],
        source_ref: str | None = None,
        relation_type: str = "supersede",
    ) -> int:
        if not old_items or not new_item or not new_item.get("id"):
            return 0
        user_id = self._user_id()
        rows: list[MemoryReplacement] = []
        for old_item in old_items:
            if not old_item or not old_item.get("id"):
                continue
            rows.append(
                MemoryReplacement(
                    user_id=user_id,
                    old_item_id=str(old_item["id"]),
                    old_memory_type=str(old_item.get("memory_type") or ""),
                    old_summary=str(old_item.get("summary") or ""),
                    old_source_ref=self._optional_str(old_item.get("source_ref")),
                    old_happened_at=self._optional_str(old_item.get("happened_at")),
                    old_extra_json=dict(old_item.get("extra_json") or {}),
                    new_item_id=str(new_item["id"]),
                    new_memory_type=str(new_item.get("memory_type") or ""),
                    new_summary=str(new_item.get("summary") or ""),
                    new_source_ref=self._optional_str(new_item.get("source_ref")),
                    new_happened_at=self._optional_str(new_item.get("happened_at")),
                    new_extra_json=dict(new_item.get("extra_json") or {}),
                    relation_type=relation_type,
                    source_ref=source_ref
                    or self._optional_str(new_item.get("source_ref")),
                )
            )
        if not rows:
            return 0
        with Session(self._engine) as db, db.begin():
            db.add_all(rows)
        return len(rows)

    def list_replacements(self) -> list[dict[str, object]]:
        user_id = self._user_id()
        with Session(self._engine) as db:
            rows = db.scalars(
                select(MemoryReplacement)
                .where(MemoryReplacement.user_id == user_id)
                .order_by(MemoryReplacement.id.asc())
            ).all()
        return [self._replacement_dict(row) for row in rows]

    def list_items_for_dashboard(
        self,
        *,
        q: str = "",
        memory_type: str = "",
        status: str = "",
        source_ref: str = "",
        scope_channel: str = "",
        scope_chat_id: str = "",
        has_embedding: bool | None = None,
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> tuple[list[dict[str, object]], int]:
        query = self._base_item_query(include_superseded=True)
        if memory_type:
            query = query.where(MemoryItem.memory_type == memory_type)
        if status:
            query = query.where(MemoryItem.status == status)
        if has_embedding is True:
            query = query.where(MemoryItem.embedding.is_not(None))
        elif has_embedding is False:
            query = query.where(MemoryItem.embedding.is_(None))
        items = self._load_items(query)

        q_lower = q.lower()
        source_lower = source_ref.lower()
        filtered = [
            item
            for item in items
            if (
                not q_lower
                or q_lower in item.id.lower()
                or q_lower in item.summary.lower()
                or q_lower in (item.source_ref or "").lower()
            )
            and (not source_lower or source_lower in (item.source_ref or "").lower())
            and self._matches_partial_scope(item, scope_channel, scope_chat_id)
        ]
        safe_sort_by = (
            sort_by
            if sort_by
            in {
                "updated_at",
                "created_at",
                "happened_at",
                "reinforcement",
                "emotional_weight",
                "memory_type",
            }
            else "created_at"
        )
        filtered.sort(key=lambda item: item.id)
        filtered.sort(
            key=lambda item: self._dashboard_sort_value(item, safe_sort_by),
            reverse=sort_order != "asc",
        )
        safe_page = max(1, page)
        safe_page_size = max(1, min(page_size, 200))
        offset = (safe_page - 1) * safe_page_size
        selected = filtered[offset : offset + safe_page_size]
        return [self._dashboard_item(item) for item in selected], len(filtered)

    def get_item_for_dashboard(
        self,
        item_id: str,
        *,
        include_embedding: bool = False,
    ) -> dict[str, object] | None:
        item = self._get_item(item_id)
        if item is None:
            return None
        embedding = list(item.embedding) if item.embedding is not None else None
        return {
            "id": item.id,
            "memory_type": item.memory_type,
            "summary": item.summary,
            "content_hash": item.content_hash,
            "reinforcement": item.reinforcement,
            "emotional_weight": item.emotional_weight,
            "extra_json": dict(item.extra_json or {}),
            "source_ref": item.source_ref,
            "happened_at": item.happened_at,
            "status": item.status,
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat(),
            "has_embedding": embedding is not None,
            "embedding_dim": len(embedding) if embedding is not None else 0,
            "embedding": embedding if include_embedding else None,
        }

    def update_item_for_dashboard(
        self,
        item_id: str,
        *,
        status: str | None = None,
        extra_json: dict[str, object] | None = None,
        source_ref: str | None = None,
        happened_at: str | None = None,
        emotional_weight: int | None = None,
    ) -> dict[str, object] | None:
        if status is not None and status.strip() not in {"active", "superseded"}:
            raise ValueError("status 仅支持 active 或 superseded")
        user_id = self._user_id()
        with Session(self._engine) as db, db.begin():
            item = db.scalar(
                select(MemoryItem)
                .where(MemoryItem.user_id == user_id, MemoryItem.id == item_id)
                .with_for_update()
            )
            if item is None:
                return None
            if status is not None:
                item.status = status.strip()
            if extra_json is not None:
                item.extra_json = dict(extra_json)
            if source_ref is not None:
                item.source_ref = source_ref
            if happened_at is not None:
                item.happened_at = happened_at
            if emotional_weight is not None:
                item.emotional_weight = _coerce_emotional_weight(emotional_weight)
            item.updated_at = utc_now()
        return self.get_item_for_dashboard(item_id)

    def replace_item_content(
        self,
        item_id: str,
        *,
        summary: str | None = None,
        memory_type: str | None = None,
        embedding: list[float] | None = None,
        replace_embedding: bool = False,
    ) -> dict[str, object] | None:
        user_id = self._user_id()
        try:
            with Session(self._engine) as db, db.begin():
                item = db.scalar(
                    select(MemoryItem)
                    .where(MemoryItem.user_id == user_id, MemoryItem.id == item_id)
                    .with_for_update()
                )
                if item is None:
                    return None
                new_summary = str(summary if summary is not None else item.summary).strip()
                new_type = str(
                    memory_type if memory_type is not None else item.memory_type
                ).strip()
                if not new_summary:
                    raise ValueError("memory summary 不能为空")
                if new_type not in {"procedure", "preference", "event", "profile"}:
                    raise ValueError(
                        "memory_type 必须是 procedure/preference/event/profile"
                    )
                item.summary = new_summary
                item.memory_type = new_type
                item.content_hash = _content_hash(new_summary, new_type)
                if replace_embedding:
                    item.embedding = embedding
                    db.flush()
                    self._sync_pgvector(db, item.id, embedding)
                item.updated_at = utc_now()
        except IntegrityError as exc:
            raise RuntimeError(
                f"memory item {item_id} 更新后 content_hash 冲突"
            ) from exc
        return self.get_item_for_dashboard(item_id)

    def delete_item(self, item_id: str) -> bool:
        return self.delete_items_batch([item_id]) > 0

    def delete_items_batch(self, ids: list[str]) -> int:
        if not ids:
            return 0
        user_id = self._user_id()
        with Session(self._engine) as db, db.begin():
            items = db.scalars(
                select(MemoryItem).where(
                    MemoryItem.user_id == user_id, MemoryItem.id.in_(ids)
                )
            ).all()
            for item in items:
                db.delete(item)
            return len(items)

    def find_similar_items_for_dashboard(
        self,
        item_id: str,
        *,
        top_k: int = 8,
        memory_type: str = "",
        score_threshold: float = 0.0,
        include_superseded: bool = False,
    ) -> list[MemoryHit]:
        base = self.get_item_for_dashboard(item_id, include_embedding=True)
        if base is None:
            raise KeyError(item_id)
        embedding = base.get("embedding")
        if not isinstance(embedding, list) or not embedding:
            raise ValueError("memory 没有 embedding")
        results = self.vector_search(
            query_vec=embedding,
            top_k=max(1, top_k) + 1,
            memory_types=[memory_type] if memory_type else None,
            score_threshold=score_threshold,
            include_superseded=include_superseded,
        )
        return [item for item in results if item.get("id") != item_id][
            : max(1, top_k)
        ]

    def vector_search(
        self,
        query_vec: list[float],
        top_k: int = 8,
        memory_types: list[str] | None = None,
        score_threshold: float = 0.0,
        include_superseded: bool = False,
        scope_channel: str | None = None,
        scope_chat_id: str | None = None,
        require_scope_match: bool = False,
        hotness_alpha: float = 0.0,
        hotness_half_life_days: float = 14.0,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
    ) -> list[MemoryHit]:
        rows = self._embedding_rows(
            query_vec=query_vec,
            top_k=top_k,
            memory_types=memory_types,
            include_superseded=include_superseded,
            scope_channel=scope_channel,
            scope_chat_id=scope_chat_id,
            require_scope_match=require_scope_match,
            time_start=time_start,
            time_end=time_end,
        )
        return score_embedding_rows(
            query_vec,
            rows,
            top_k=top_k,
            score_threshold=score_threshold,
            hotness_alpha=hotness_alpha,
            hotness_half_life_days=hotness_half_life_days,
        )

    def vector_search_batch(
        self,
        query_vecs: list[list[float]],
        top_k: int = 8,
        memory_types: list[str] | None = None,
        score_threshold: float = 0.0,
        include_superseded: bool = False,
        scope_channel: str | None = None,
        scope_chat_id: str | None = None,
        require_scope_match: bool = False,
        hotness_alpha: float = 0.0,
        hotness_half_life_days: float = 14.0,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
    ) -> list[list[MemoryHit]]:
        if not query_vecs:
            return []
        return [
            score_embedding_rows(
                vector,
                self._embedding_rows(
                    query_vec=vector,
                    top_k=top_k,
                    memory_types=memory_types,
                    include_superseded=include_superseded,
                    scope_channel=scope_channel,
                    scope_chat_id=scope_chat_id,
                    require_scope_match=require_scope_match,
                    time_start=time_start,
                    time_end=time_end,
                ),
                top_k=top_k,
                score_threshold=score_threshold,
                hotness_alpha=hotness_alpha,
                hotness_half_life_days=hotness_half_life_days,
            )
            for vector in query_vecs
        ]

    def keyword_search_summary(
        self,
        terms: list[str],
        memory_types: list[str] | None = None,
        limit: int = 20,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
        scope_channel: str | None = None,
        scope_chat_id: str | None = None,
        require_scope_match: bool = False,
    ) -> list[MemoryHit]:
        clean_terms = [term for term in terms if term and len(term) >= 2]
        if not clean_terms:
            return []
        query = self._base_item_query(include_superseded=False).where(
            or_(*(MemoryItem.summary.contains(term) for term in clean_terms))
        )
        if memory_types:
            query = query.where(MemoryItem.memory_type.in_(memory_types))
        items = self._load_items(query)
        ranked: list[tuple[int, int, str, MemoryHit]] = []
        for item in items:
            if not self._matches_scope(
                item, scope_channel, scope_chat_id, require_scope_match
            ) or (
                (time_start is not None or time_end is not None)
                and not _is_memory_time_in_range(
                    item.happened_at, time_start, time_end
                )
            ):
                continue
            matches = sum(1 for term in clean_terms if term in item.summary)
            if not matches:
                continue
            ranked.append(
                (
                    -matches,
                    -item.reinforcement,
                    item.id,
                    {
                        "id": item.id,
                        "memory_type": item.memory_type,
                        "summary": item.summary,
                        "source_ref": item.source_ref or "",
                        "happened_at": item.happened_at or item.created_at.isoformat(),
                        "keyword_score": matches / len(clean_terms),
                    },
                )
            )
        ranked.sort(key=lambda entry: entry[:3])
        return [entry[3] for entry in ranked[:limit]]

    def find_similar_recent_events(
        self,
        embedding: list[float],
        *,
        days_back: int = 7,
        threshold: float = 0.92,
        top_k: int = 3,
    ) -> list[str]:
        cutoff = datetime.now(UTC) - timedelta(days=max(1, int(days_back)))
        items = self._load_items(
            self._base_item_query(include_superseded=False).where(
                MemoryItem.memory_type == "event",
                MemoryItem.embedding.is_not(None),
                MemoryItem.created_at >= cutoff,
            )
        )
        scored = [
            (item.id, _cosine_similarity(embedding, item.embedding or []))
            for item in items
        ]
        scored = [item for item in scored if item[1] >= float(threshold)]
        scored.sort(key=lambda item: item[1], reverse=True)
        return [item_id for item_id, _ in scored[: max(1, int(top_k))]]

    def list_by_type(self, memory_type: str) -> list[dict[str, object]]:
        items = self._load_items(
            self._base_item_query(include_superseded=True).where(
                MemoryItem.memory_type == memory_type
            )
        )
        return [
            {
                "id": item.id,
                "memory_type": item.memory_type,
                "summary": item.summary,
                "extra_json": dict(item.extra_json or {}),
                "happened_at": item.happened_at,
                "reinforcement": item.reinforcement,
                "emotional_weight": item.emotional_weight,
            }
            for item in items
        ]

    def list_events_by_time_range(
        self,
        time_start: datetime,
        time_end: datetime,
        limit: int = 200,
    ) -> list[dict[str, object]]:
        items = self._load_items(
            self._base_item_query(include_superseded=False).where(
                MemoryItem.memory_type == "event"
            )
        )
        hits: list[tuple[datetime, dict[str, object]]] = []
        for item in items:
            parsed_time = _parse_memory_time(item.happened_at)
            if parsed_time is None or parsed_time < time_start or parsed_time >= time_end:
                continue
            hits.append(
                (
                    parsed_time,
                    {
                        "id": item.id,
                        "memory_type": item.memory_type,
                        "summary": item.summary,
                        "source_ref": item.source_ref or "",
                        "happened_at": item.happened_at or "",
                        "score": 1.0,
                    },
                )
            )
        max_items = max(1, min(limit, 200))
        hits.sort(key=lambda entry: entry[0], reverse=True)
        selected = hits[:max_items]
        selected.sort(key=lambda entry: entry[0])
        return [item for _, item in selected]

    def delete_by_source_ref(self, source_ref: str) -> int:
        user_id = self._user_id()
        with Session(self._engine) as db, db.begin():
            items = db.scalars(
                select(MemoryItem).where(
                    MemoryItem.user_id == user_id,
                    MemoryItem.source_ref == source_ref,
                )
            ).all()
            for item in items:
                db.delete(item)
            return len(items)

    def has_item_by_source_ref(
        self,
        source_ref: str,
        memory_type: str | None = None,
    ) -> bool:
        query = self._base_item_query(include_superseded=True).where(
            MemoryItem.source_ref == source_ref
        )
        if memory_type:
            query = query.where(MemoryItem.memory_type == memory_type)
        with Session(self._engine) as db:
            return db.scalar(query.with_only_columns(MemoryItem.id).limit(1)) is not None

    def keyword_match_procedures(
        self, action_tokens: list[str]
    ) -> list[dict[str, object]]:
        if not action_tokens:
            return []
        token_set = {token.lower() for token in action_tokens if token}
        action_text = " ".join(action_tokens).lower()
        items = self._load_items(
            self._base_item_query(include_superseded=False).where(
                MemoryItem.memory_type == "procedure",
                MemoryItem.extra_json.is_not(None),
            )
        )
        matched: list[dict[str, object]] = []
        for item in items:
            extra = dict(item.extra_json or {})
            raw_tags = extra.get("trigger_tags") or {}
            if not isinstance(raw_tags, dict) or raw_tags.get("scope") != "tool_triggered":
                continue
            keywords = [
                str(keyword)
                for keyword in (raw_tags.get("keywords") or [])
                if keyword and len(str(keyword)) >= 3
            ]
            if keywords:
                hit = any(keyword.lower() in action_text for keyword in keywords)
            else:
                procedure_tools = [
                    str(value) for value in (raw_tags.get("tools") or [])
                ]
                procedure_skills = [
                    str(value) for value in (raw_tags.get("skills") or [])
                ]
                if len(procedure_tools) > 4:
                    continue
                tag_tokens = {value.lower() for value in procedure_tools}
                tag_tokens |= {value.lower() for value in procedure_skills}
                hit = bool(token_set & tag_tokens)
            if hit:
                matched.append(
                    {
                        "id": item.id,
                        "memory_type": "procedure",
                        "summary": item.summary,
                        "extra_json": extra,
                        "intercept": bool(raw_tags.get("intercept", False)),
                        "score": 1.0,
                    }
                )
        return matched

    def undo_by_message_sources(
        self,
        message_ids: list[str],
        *,
        dry_run: bool = False,
    ) -> dict[str, object]:
        clean_ids = [str(item).strip() for item in message_ids if str(item).strip()]
        if not clean_ids:
            return {"affected_ids": [], "restored_ids": [], "rollback_source_ids": []}
        targets = set(clean_ids)
        user_id = self._user_id()
        with Session(self._engine) as db, db.begin():
            items = db.scalars(
                select(MemoryItem).where(
                    MemoryItem.user_id == user_id,
                    MemoryItem.source_ref.is_not(None),
                )
            ).all()
            affected_ids: set[str] = set()
            rollback_source_ids: set[str] = set()
            for item in items:
                source = (item.source_ref or "").strip()
                base_ids = self._source_ref_message_ids(source)
                if source in targets:
                    affected_ids.add(item.id)
                    rollback_source_ids.add(source)
                elif base_ids and targets.intersection(base_ids):
                    affected_ids.add(item.id)
                    rollback_source_ids.update(base_ids)

            if affected_ids and not dry_run:
                now = utc_now()
                for item in items:
                    if item.id in affected_ids:
                        item.status = "superseded"
                        item.updated_at = now

            replacements = db.scalars(
                select(MemoryReplacement).where(
                    MemoryReplacement.user_id == user_id,
                    MemoryReplacement.new_item_id.in_(affected_ids or {""}),
                )
            ).all()
            old_ids = {replacement.old_item_id for replacement in replacements}
            restored_ids: set[str] = set()
            for old_id in sorted(old_ids):
                active_replacement = db.scalar(
                    select(MemoryReplacement.id)
                    .join(
                        MemoryItem,
                        (MemoryItem.user_id == MemoryReplacement.user_id)
                        & (MemoryItem.id == MemoryReplacement.new_item_id),
                    )
                    .where(
                        MemoryReplacement.user_id == user_id,
                        MemoryReplacement.old_item_id == old_id,
                        MemoryReplacement.new_item_id.not_in(affected_ids or {""}),
                        MemoryItem.status == "active",
                    )
                    .limit(1)
                )
                if active_replacement is not None:
                    continue
                old_item = db.scalar(
                    select(MemoryItem).where(
                        MemoryItem.user_id == user_id,
                        MemoryItem.id == old_id,
                        MemoryItem.status == "superseded",
                    )
                )
                if old_item is None:
                    continue
                restored_ids.add(old_id)
                if not dry_run:
                    old_item.status = "active"
                    old_item.updated_at = utc_now()

        return {
            "affected_ids": sorted(affected_ids),
            "restored_ids": sorted(restored_ids),
            "rollback_source_ids": sorted(rollback_source_ids),
        }

    def get_item_merge_metadata(
        self, item_id: str
    ) -> tuple[str, dict[str, object]]:
        item = self._get_item(item_id)
        if item is None:
            raise KeyError(f"找不到 memory item: {item_id}")
        return item.memory_type, dict(item.extra_json or {})

    def merge_item_raw(
        self,
        item_id: str,
        new_summary: str,
        new_embedding: list[float],
        new_extra: dict[str, object] | None = None,
    ) -> None:
        user_id = self._user_id()
        try:
            with Session(self._engine) as db, db.begin():
                item = db.scalar(
                    select(MemoryItem)
                    .where(MemoryItem.user_id == user_id, MemoryItem.id == item_id)
                    .with_for_update()
                )
                if item is None:
                    raise KeyError(f"找不到 memory item: {item_id}")
                item.summary = new_summary
                item.content_hash = _content_hash(new_summary, item.memory_type)
                item.embedding = new_embedding
                db.flush()
                self._sync_pgvector(db, item.id, new_embedding)
                if new_extra is not None:
                    item.extra_json = dict(new_extra)
                item.reinforcement += 1
                item.updated_at = utc_now()
        except IntegrityError as exc:
            raise RuntimeError(
                f"memory item {item_id} 合并后 content_hash 冲突"
            ) from exc

    def _base_item_query(self, *, include_superseded: bool) -> Select[tuple[MemoryItem]]:
        query = select(MemoryItem).where(MemoryItem.user_id == self._user_id())
        if not include_superseded:
            query = query.where(MemoryItem.status == "active")
        return query

    @staticmethod
    def _reinforce_item(
        item: MemoryItem,
        emotional_weight: int,
        *,
        happened_at: str | None = None,
    ) -> None:
        item.status = "active"
        item.reinforcement += 1
        item.emotional_weight = max(item.emotional_weight, emotional_weight)
        if happened_at is not None:
            item.happened_at = item.happened_at or happened_at
        item.updated_at = utc_now()

    def _load_items(self, query: Select[tuple[MemoryItem]]) -> list[MemoryItem]:
        with Session(self._engine) as db:
            return list(db.scalars(query).all())

    def _get_item(self, item_id: str) -> MemoryItem | None:
        with Session(self._engine) as db:
            return db.scalar(
                select(MemoryItem).where(
                    MemoryItem.user_id == self._user_id(), MemoryItem.id == item_id
                )
            )

    def _embedding_rows(
        self,
        *,
        query_vec: list[float],
        top_k: int,
        memory_types: list[str] | None,
        include_superseded: bool,
        scope_channel: str | None,
        scope_chat_id: str | None,
        require_scope_match: bool,
        time_start: datetime | None,
        time_end: datetime | None,
    ) -> list[EmbeddingRow]:
        query = self._base_item_query(include_superseded=include_superseded).where(
            MemoryItem.embedding.is_not(None)
        )
        if memory_types:
            query = query.where(MemoryItem.memory_type.in_(memory_types))
        candidate_ids = self._pgvector_candidate_ids(
            query_vec,
            top_k=top_k,
            memory_types=memory_types,
            include_superseded=include_superseded,
        )
        if candidate_ids is not None:
            if not candidate_ids:
                return []
            query = query.where(MemoryItem.id.in_(candidate_ids))
        items = self._load_items(query)
        rows: list[EmbeddingRow] = []
        for item in items:
            if not self._matches_scope(
                item, scope_channel, scope_chat_id, require_scope_match
            ) or (
                (time_start is not None or time_end is not None)
                and not _is_memory_time_in_range(
                    item.happened_at, time_start, time_end
                )
            ):
                continue
            extra = dict(item.extra_json or {})
            extra["_reinforcement"] = item.reinforcement
            extra["_updated_at"] = item.updated_at.isoformat()
            extra["_emotional_weight"] = item.emotional_weight
            rows.append(
                (
                    item.id,
                    item.memory_type,
                    item.summary,
                    list(item.embedding) if item.embedding is not None else None,
                    extra,
                    item.happened_at,
                    item.source_ref,
                )
            )
        return rows

    def _pgvector_candidate_ids(
        self,
        query_vec: list[float],
        *,
        top_k: int,
        memory_types: list[str] | None,
        include_superseded: bool,
    ) -> list[str] | None:
        """Use pgvector only for recall; canonical Python scoring remains authoritative."""
        if self._engine.dialect.name != "postgresql":
            return None
        if len(query_vec) != self._vector_dimension:
            raise ValueError(
                "Cloud embedding dimension mismatch: "
                f"expected={self._vector_dimension} actual={len(query_vec)}"
            )
        conditions = [
            "user_id = :user_id",
            "embedding_vector IS NOT NULL",
        ]
        candidate_limit = max(
            self.VECTOR_CANDIDATE_FLOOR,
            max(1, top_k) * self.VECTOR_CANDIDATE_MULTIPLIER,
        )
        params: dict[str, object] = {
            "user_id": self._user_id(),
            "query_vector": self._vector_literal(query_vec),
            "candidate_limit": candidate_limit,
        }
        if not include_superseded:
            conditions.append("status = 'active'")
        count_query = select(func.count(MemoryItem.id)).where(
            MemoryItem.user_id == self._user_id(),
            MemoryItem.embedding.is_not(None),
        )
        if not include_superseded:
            count_query = count_query.where(MemoryItem.status == "active")
        if memory_types:
            count_query = count_query.where(MemoryItem.memory_type.in_(memory_types))
        with Session(self._engine) as db:
            eligible = int(db.scalar(count_query) or 0)
        if eligible <= candidate_limit:
            # Exact canonical scoring is cheap and avoids ANN recall loss for a
            # normal-sized user's memory set.
            return None

        statement = None
        if memory_types:
            conditions.append("memory_type IN :memory_types")
            params["memory_types"] = list(memory_types)
            statement = text(
                "SELECT id FROM memory_items WHERE "
                + " AND ".join(conditions)
                + " ORDER BY embedding_vector <=> CAST(:query_vector AS vector(1024)) "
                + "LIMIT :candidate_limit"
            ).bindparams(bindparam("memory_types", expanding=True))
        else:
            statement = text(
                "SELECT id FROM memory_items WHERE "
                + " AND ".join(conditions)
                + " ORDER BY embedding_vector <=> CAST(:query_vector AS vector(1024)) "
                + "LIMIT :candidate_limit"
            )
        with Session(self._engine) as db:
            return [str(row[0]) for row in db.execute(statement, params)]

    def _sync_pgvector(
        self, db: Session, item_id: str, embedding: list[float] | None
    ) -> None:
        if self._engine.dialect.name != "postgresql":
            return
        if embedding is not None and len(embedding) != self._vector_dimension:
            raise ValueError(
                "Cloud embedding dimension mismatch: "
                f"expected={self._vector_dimension} actual={len(embedding)}"
            )
        db.execute(
            text(
                "UPDATE memory_items "
                "SET embedding_vector = CAST(:embedding AS vector(1024)) "
                "WHERE user_id = :user_id AND id = :item_id"
            ),
            {
                "embedding": self._vector_literal(embedding)
                if embedding is not None
                else None,
                "user_id": self._user_id(),
                "item_id": item_id,
            },
        )

    @staticmethod
    def _vector_literal(vector: list[float] | None) -> str | None:
        return json.dumps(vector, separators=(",", ":")) if vector is not None else None

    @staticmethod
    def _matches_scope(
        item: MemoryItem,
        scope_channel: str | None,
        scope_chat_id: str | None,
        require_scope_match: bool,
    ) -> bool:
        if not require_scope_match:
            return True
        extra = item.extra_json or {}
        return (
            str(extra.get("scope_channel") or "").strip()
            == (scope_channel or "").strip()
            and str(extra.get("scope_chat_id") or "").strip()
            == (scope_chat_id or "").strip()
        )

    @staticmethod
    def _matches_partial_scope(
        item: MemoryItem,
        scope_channel: str,
        scope_chat_id: str,
    ) -> bool:
        extra = item.extra_json or {}
        return (
            not scope_channel
            or str(extra.get("scope_channel") or "").strip()
            == scope_channel.strip()
        ) and (
            not scope_chat_id
            or str(extra.get("scope_chat_id") or "").strip()
            == scope_chat_id.strip()
        )

    @staticmethod
    def _dashboard_sort_value(item: MemoryItem, sort_by: str) -> object:
        value = getattr(item, sort_by)
        if value is None:
            return "" if sort_by in {"happened_at", "memory_type"} else 0
        return value

    @staticmethod
    def _dashboard_item(item: MemoryItem) -> dict[str, object]:
        extra = item.extra_json or {}
        return {
            "id": item.id,
            "memory_type": item.memory_type,
            "summary": item.summary,
            "source_ref": item.source_ref,
            "happened_at": item.happened_at,
            "status": item.status,
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat(),
            "reinforcement": item.reinforcement,
            "emotional_weight": item.emotional_weight,
            "has_embedding": item.embedding is not None,
            "scope_channel": extra.get("scope_channel", ""),
            "scope_chat_id": extra.get("scope_chat_id", ""),
        }

    @staticmethod
    def _replacement_dict(row: MemoryReplacement) -> dict[str, object]:
        return {
            "old_item_id": row.old_item_id,
            "old_memory_type": row.old_memory_type,
            "old_summary": row.old_summary,
            "old_source_ref": row.old_source_ref,
            "old_happened_at": row.old_happened_at,
            "old_extra_json": dict(row.old_extra_json or {}),
            "new_item_id": row.new_item_id,
            "new_memory_type": row.new_memory_type,
            "new_summary": row.new_summary,
            "new_source_ref": row.new_source_ref,
            "new_happened_at": row.new_happened_at,
            "new_extra_json": dict(row.new_extra_json or {}),
            "relation_type": row.relation_type,
            "source_ref": row.source_ref,
            "created_at": row.created_at.isoformat(),
        }

    @staticmethod
    def _optional_str(value: object) -> str | None:
        return str(value) if value is not None else None

    @staticmethod
    def _source_ref_message_ids(source_ref: str) -> list[str]:
        raw = str(source_ref or "").strip()
        if not raw:
            return []
        base = raw.split("#", 1)[0].strip()
        if not base.startswith("["):
            return []
        try:
            loaded = json.loads(base)
        except json.JSONDecodeError:
            return []
        if not isinstance(loaded, list):
            return []
        return [str(item).strip() for item in loaded if str(item).strip()]

    @staticmethod
    def _item_dict(item: MemoryItem) -> dict[str, object]:
        return {
            "id": item.id,
            "memory_type": item.memory_type,
            "summary": item.summary,
            "extra_json": dict(item.extra_json or {}),
            "source_ref": item.source_ref,
            "happened_at": item.happened_at,
            "status": item.status,
            "created_at": item.created_at.isoformat(),
            "updated_at": item.updated_at.isoformat(),
            "emotional_weight": item.emotional_weight,
        }
