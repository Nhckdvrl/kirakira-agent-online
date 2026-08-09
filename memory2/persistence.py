"""Persistence port consumed by the unchanged Memory v2 algorithms."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from memory2.store import MemoryHit


class MemoryPersistence(Protocol):
    """Storage operations required by ``Memorizer`` and ``Retriever``.

    The algorithms deliberately depend on this narrow synchronous port.  Local
    mode can keep using SQLite while Cloud binds a user-scoped PostgreSQL store.
    """

    def upsert_item(
        self,
        memory_type: str,
        summary: str,
        embedding: list[float] | None,
        source_ref: str | None = None,
        extra: dict[str, object] | None = None,
        happened_at: str | None = None,
        emotional_weight: int = 0,
    ) -> str: ...

    def upsert_consolidation_event(
        self,
        *,
        source_ref: str,
        summary: str,
        embedding: list[float] | None,
        extra: dict[str, object] | None = None,
        happened_at: str | None = None,
        emotional_weight: int = 0,
    ) -> str: ...

    def has_consolidation_source_ref(self, source_ref: str) -> bool: ...
    def mark_superseded_batch(self, ids: list[str]) -> None: ...
    def get_items_by_ids(self, ids: list[str]) -> list[dict[str, object]]: ...

    def reinforce_items_batch(
        self, ids: list[str], emotional_weight: int = 0
    ) -> None: ...

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
    ) -> list[MemoryHit]: ...

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
    ) -> list[list[MemoryHit]]: ...

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
    ) -> list[MemoryHit]: ...

    def find_similar_recent_events(
        self,
        embedding: list[float],
        *,
        days_back: int = 7,
        threshold: float = 0.92,
        top_k: int = 3,
    ) -> list[str]: ...

    def get_item_merge_metadata(
        self, item_id: str
    ) -> tuple[str, dict[str, object]]: ...

    def merge_item_raw(
        self,
        item_id: str,
        new_summary: str,
        new_embedding: list[float],
        new_extra: dict[str, object] | None = None,
    ) -> None: ...

    def record_replacements(
        self,
        *,
        old_items: list[dict[str, object]],
        new_item: dict[str, object],
        source_ref: str | None = None,
        relation_type: str = "supersede",
    ) -> int: ...

    def list_replacements(self) -> list[dict[str, object]]: ...

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
    ) -> tuple[list[dict[str, object]], int]: ...

    def get_item_for_dashboard(
        self, item_id: str, *, include_embedding: bool = False
    ) -> dict[str, object] | None: ...

    def update_item_for_dashboard(
        self,
        item_id: str,
        *,
        status: str | None = None,
        extra_json: dict[str, object] | None = None,
        source_ref: str | None = None,
        happened_at: str | None = None,
        emotional_weight: int | None = None,
    ) -> dict[str, object] | None: ...

    def delete_item(self, item_id: str) -> bool: ...
    def delete_items_batch(self, ids: list[str]) -> int: ...

    def find_similar_items_for_dashboard(
        self,
        item_id: str,
        *,
        top_k: int = 8,
        memory_type: str = "",
        score_threshold: float = 0.0,
        include_superseded: bool = False,
    ) -> list[MemoryHit]: ...

    def list_events_by_time_range(
        self, time_start: datetime, time_end: datetime, limit: int = 200
    ) -> list[dict[str, object]]: ...

    def keyword_match_procedures(
        self, action_tokens: list[str]
    ) -> list[dict[str, object]]: ...

    def undo_by_message_sources(
        self, message_ids: list[str], *, dry_run: bool = False
    ) -> dict[str, object]: ...
