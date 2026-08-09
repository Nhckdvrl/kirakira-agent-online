from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from cloud.markdown_memory import UserScopedPostgresMarkdownStore
from cloud.models import Base, User
from core.memory.store import DEFAULT_SELF_MD


@pytest.fixture
def profile_store() -> tuple[UserScopedPostgresMarkdownStore, str, str]:
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
                User(id=first_id, email="profile-1@example.test", password_hash="x"),
                User(id=second_id, email="profile-2@example.test", password_hash="x"),
            ]
        )
    return UserScopedPostgresMarkdownStore(engine), str(first_id), str(second_id)


def test_profile_store_fails_closed_without_user(
    profile_store: tuple[UserScopedPostgresMarkdownStore, str, str],
) -> None:
    store, _first_id, _second_id = profile_store
    with pytest.raises(RuntimeError, match="active user binding"):
        store.read_long_term()


def test_profile_documents_and_append_idempotency_are_user_scoped(
    profile_store: tuple[UserScopedPostgresMarkdownStore, str, str],
) -> None:
    store, first_id, second_id = profile_store
    with store.bind_user(first_id):
        assert store.read_self() == DEFAULT_SELF_MD
        store.write_long_term("first profile")
        store.write_recent_context("first recent")
        assert store.append_pending_once(
            "- [preference] coffee", source_ref="turn-1"
        )
        assert not store.append_pending_once(
            "- [preference] coffee", source_ref="turn-1"
        )
        assert store.read_pending().count("coffee") == 1

    with store.bind_user(second_id):
        assert store.read_long_term() == ""
        assert store.read_recent_context() == ""
        assert store.append_pending_once(
            "- [preference] coffee", source_ref="turn-1"
        )

    with store.bind_user(first_id):
        assert store.read_long_term() == "first profile"
        assert store.read_pending().count("coffee") == 1


def test_pending_snapshot_commit_and_rollback_preserve_new_appends(
    profile_store: tuple[UserScopedPostgresMarkdownStore, str, str],
) -> None:
    store, first_id, _second_id = profile_store
    with store.bind_user(first_id):
        store.append_pending("old fact")
        assert store.snapshot_pending().strip() == "old fact"
        store.append_pending("new fact")
        store.rollback_pending_snapshot()
        assert store.read_pending().splitlines() == ["old fact", "new fact"]

        assert store.snapshot_pending().splitlines() == ["old fact", "new fact"]
        store.commit_pending_snapshot()
        assert store.read_pending() == ""


def test_profile_backups_and_context_match_local_contract(
    profile_store: tuple[UserScopedPostgresMarkdownStore, str, str],
) -> None:
    store, first_id, _second_id = profile_store
    with store.bind_user(first_id):
        store.write_long_term("stable facts")
        store.backup_long_term()
        assert store.has_long_term_memory()
        assert store.get_memory_context() == "## Long-term Memory\nstable facts"
