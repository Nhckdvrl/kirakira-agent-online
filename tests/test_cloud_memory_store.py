from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from cloud.memory_store import UserScopedPostgresMemoryStore
from cloud.models import Base, User
from core.memory.engine import MemoryQuery, MemoryScope
from memory2.memorizer import Memorizer
from memory2.retriever import Retriever
from plugins.default_memory.engine import DefaultMemoryEngine


class _Embedder:
    async def embed(self, text: str) -> list[float]:
        if "coffee" in text.lower() or "咖啡" in text:
            return [1.0, 0.0, 0.0]
        return [0.0, 1.0, 0.0]


@pytest.fixture
def cloud_memory() -> tuple[UserScopedPostgresMemoryStore, str, str]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    first_id = uuid4()
    second_id = uuid4()
    with Session(engine) as db, db.begin():
        db.add_all(
            [
                User(
                    id=first_id,
                    email="first@example.test",
                    password_hash="unused",
                ),
                User(
                    id=second_id,
                    email="second@example.test",
                    password_hash="unused",
                ),
            ]
        )
    return UserScopedPostgresMemoryStore(engine), str(first_id), str(second_id)


def test_unbound_access_fails_closed(
    cloud_memory: tuple[UserScopedPostgresMemoryStore, str, str],
) -> None:
    store, _first_id, _second_id = cloud_memory

    with pytest.raises(RuntimeError, match="active user binding"):
        store.vector_search([1.0, 0.0, 0.0])


def test_same_memory_is_deduplicated_per_user_not_globally(
    cloud_memory: tuple[UserScopedPostgresMemoryStore, str, str],
) -> None:
    store, first_id, second_id = cloud_memory

    with store.bind_user(first_id):
        first_write = store.upsert_item(
            "preference", "I prefer coffee", [1.0, 0.0, 0.0]
        )
        first_reinforcement = store.upsert_item(
            "preference", "I prefer coffee", [1.0, 0.0, 0.0]
        )
        first_hits = store.vector_search([1.0, 0.0, 0.0])

    with store.bind_user(second_id):
        second_write = store.upsert_item(
            "preference", "I prefer coffee", [1.0, 0.0, 0.0]
        )
        second_hits = store.vector_search([1.0, 0.0, 0.0])

    first_item_id = first_write.removeprefix("new:")
    second_item_id = second_write.removeprefix("new:")
    assert first_reinforcement == f"reinforced:{first_item_id}"
    assert second_item_id != first_item_id
    assert [hit["id"] for hit in first_hits] == [first_item_id]
    assert [hit["id"] for hit in second_hits] == [second_item_id]


def test_consolidation_idempotency_is_user_scoped(
    cloud_memory: tuple[UserScopedPostgresMemoryStore, str, str],
) -> None:
    store, first_id, second_id = cloud_memory

    with store.bind_user(first_id):
        first = store.upsert_consolidation_event(
            source_ref="turn-1",
            summary="[2026-08-09] drank coffee",
            embedding=[1.0, 0.0, 0.0],
        )
        duplicate = store.upsert_consolidation_event(
            source_ref="turn-1",
            summary="[2026-08-09] drank coffee",
            embedding=[1.0, 0.0, 0.0],
        )

    with store.bind_user(second_id):
        second = store.upsert_consolidation_event(
            source_ref="turn-1",
            summary="[2026-08-09] drank coffee",
            embedding=[1.0, 0.0, 0.0],
        )

    assert first.startswith("new:")
    assert duplicate == f"skipped:{first.removeprefix('new:')}"
    assert second.startswith("new:")
    assert second != first


@pytest.mark.asyncio
async def test_original_memorizer_and_retriever_run_on_cloud_store(
    cloud_memory: tuple[UserScopedPostgresMemoryStore, str, str],
) -> None:
    store, first_id, second_id = cloud_memory
    embedder = _Embedder()
    memorizer = Memorizer(store, embedder)  # type: ignore[arg-type]
    retriever = Retriever(
        store,  # type: ignore[arg-type]
        embedder,  # type: ignore[arg-type]
        score_threshold=0.2,
        hotness_alpha=0.2,
    )

    with store.bind_user(first_id):
        result = await memorizer.save_item_with_supersede(
            summary="I prefer coffee",
            memory_type="preference",
            extra={"scope_channel": "user", "scope_chat_id": first_id},
            source_ref="message-1",
        )
        hits = await retriever.retrieve(
            "coffee preference",
            memory_types=["preference"],
            scope_channel="user",
            scope_chat_id=first_id,
            require_scope_match=True,
        )

    with store.bind_user(second_id):
        isolated_hits = await retriever.retrieve(
            "coffee preference", memory_types=["preference"]
        )

    assert result.startswith("new:")
    assert [hit["id"] for hit in hits] == [result.removeprefix("new:")]
    assert hits[0]["rrf_score"] > 0
    assert hits[0]["_score_debug"]["hotness"] > 0
    assert isolated_hits == []


@pytest.mark.asyncio
async def test_default_engine_binds_memory_scope_user_for_cloud_store(
    cloud_memory: tuple[UserScopedPostgresMemoryStore, str, str],
) -> None:
    store, first_id, _second_id = cloud_memory
    embedder = _Embedder()
    with store.bind_user(first_id):
        store.upsert_item(
            "preference",
            "I prefer coffee",
            [1.0, 0.0, 0.0],
            extra={"scope_channel": "user", "scope_chat_id": first_id},
        )

    engine = DefaultMemoryEngine.__new__(DefaultMemoryEngine)
    engine._v2_store = store
    engine._retriever = Retriever(
        store,  # type: ignore[arg-type]
        embedder,  # type: ignore[arg-type]
        score_threshold=0.2,
    )

    result = await engine.query(
        MemoryQuery(
            text="coffee",
            intent="context",
            scope=MemoryScope(
                session_key="conversation-1",
                channel="user",
                chat_id=first_id,
                user_id=first_id,
            ),
        )
    )
    assert len(result.records) == 1

    with pytest.raises(RuntimeError, match="MemoryScope.user_id"):
        await engine.query(
            MemoryQuery(text="coffee", intent="context", scope=MemoryScope())
        )


def test_cloud_memory_admin_operations_remain_user_scoped(
    cloud_memory: tuple[UserScopedPostgresMemoryStore, str, str],
) -> None:
    store, first_id, second_id = cloud_memory
    with store.bind_user(first_id):
        first = store.upsert_item(
            "preference",
            "coffee preference",
            [1.0, 0.0, 0.0],
            source_ref="source-first",
            extra={"scope_channel": "user", "scope_chat_id": first_id},
        ).removeprefix("new:")
        items, total = store.list_items_for_dashboard(q="coffee")
        assert total == 1
        assert [item["id"] for item in items] == [first]
        updated = store.update_item_for_dashboard(first, emotional_weight=8)
        assert updated is not None and updated["emotional_weight"] == 8
        replaced = store.replace_item_content(first, summary="strong coffee preference")
        assert replaced is not None and replaced["summary"] == "strong coffee preference"
        assert store.find_similar_items_for_dashboard(first) == []

    with store.bind_user(second_id):
        second = store.upsert_item(
            "preference",
            "coffee preference",
            [1.0, 0.0, 0.0],
            source_ref="source-second",
        ).removeprefix("new:")
        assert store.get_item_for_dashboard(first) is None
        assert not store.delete_item(first)
        assert store.delete_item(second)

    with store.bind_user(first_id):
        assert store.get_item_for_dashboard(first) is not None


def test_timeline_and_procedure_keyword_lanes_preserve_behavior(
    cloud_memory: tuple[UserScopedPostgresMemoryStore, str, str],
) -> None:
    store, first_id, _second_id = cloud_memory
    with store.bind_user(first_id):
        event = store.upsert_item(
            "event",
            "released the Cloud adapter",
            [0.0, 1.0, 0.0],
            happened_at="2026-08-09T12:00:00+00:00",
        ).removeprefix("new:")
        procedure = store.upsert_item(
            "procedure",
            "use the deployment checklist",
            [0.0, 1.0, 0.0],
            extra={
                "trigger_tags": {
                    "scope": "tool_triggered",
                    "keywords": ["deploy"],
                    "tools": ["shell"],
                    "intercept": True,
                }
            },
        ).removeprefix("new:")
        timeline = store.list_events_by_time_range(
            datetime(2026, 8, 9, tzinfo=UTC),
            datetime(2026, 8, 10, tzinfo=UTC),
        )
        procedures = store.keyword_match_procedures(["shell", "deploy"])

    assert [item["id"] for item in timeline] == [event]
    assert [item["id"] for item in procedures] == [procedure]
    assert procedures[0]["intercept"] is True


def test_replacement_undo_restores_only_bound_users_memory(
    cloud_memory: tuple[UserScopedPostgresMemoryStore, str, str],
) -> None:
    store, first_id, second_id = cloud_memory
    with store.bind_user(first_id):
        old_id = store.upsert_item(
            "profile", "old status", [0.0, 1.0, 0.0], source_ref="old-message"
        ).removeprefix("new:")
        old_item = store.get_items_by_ids([old_id])[0]
        store.mark_superseded_batch([old_id])
        new_id = store.upsert_item(
            "profile",
            "new status",
            [0.0, 1.0, 0.0],
            source_ref='["message-1"]#h:abc',
        ).removeprefix("new:")
        new_item = store.get_items_by_ids([new_id])[0]
        assert store.record_replacements(old_items=[old_item], new_item=new_item) == 1
        assert len(store.list_replacements()) == 1
        preview = store.undo_by_message_sources(["message-1"], dry_run=True)
        assert preview["affected_ids"] == [new_id]
        assert preview["restored_ids"] == [old_id]
        old_detail = store.get_item_for_dashboard(old_id)
        assert old_detail is not None and old_detail["status"] == "superseded"
        applied = store.undo_by_message_sources(["message-1"])
        assert applied == preview
        old_detail = store.get_item_for_dashboard(old_id)
        new_detail = store.get_item_for_dashboard(new_id)
        assert old_detail is not None and old_detail["status"] == "active"
        assert new_detail is not None and new_detail["status"] == "superseded"

    with store.bind_user(second_id):
        other_id = store.upsert_item(
            "profile",
            "other user status",
            [0.0, 1.0, 0.0],
            source_ref='["message-1"]#h:other',
        ).removeprefix("new:")
        assert store.undo_by_message_sources(["message-1"])["affected_ids"] == [
            other_id
        ]

    with store.bind_user(first_id):
        new_detail = store.get_item_for_dashboard(new_id)
        assert new_detail is not None and new_detail["status"] == "superseded"
