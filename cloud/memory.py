"""Cloud composition for the unchanged Default Memory algorithms."""

from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, cast

from sqlalchemy import Engine

from agent.config_models import build_config
from cloud.database import CloudSettings, build_sync_engine
from cloud.memory_store import UserScopedPostgresMemoryStore
from core.memory.engine import MemoryEngineDescriptor
from core.memory.services import MemoryServices
from core.net.http import SharedHttpResources
from infra.providers.model_client_adapter import LLMProvider
from plugins.default_memory.config import DefaultMemoryConfig
from plugins.default_memory.engine import DefaultMemoryEngine
from memory2.store import VEC_DIM


class CloudDefaultMemoryEngine(DefaultMemoryEngine):
    """Default MemoryEngine with durable/user-scoped deployment metadata."""

    DESCRIPTOR = MemoryEngineDescriptor(
        name="default-cloud",
        profile=DefaultMemoryEngine.DESCRIPTOR.profile,
        capabilities=DefaultMemoryEngine.DESCRIPTOR.capabilities,
        notes={
            **DefaultMemoryEngine.DESCRIPTOR.notes,
            "durable_backend": "postgresql",
            "tenant_scope": "user",
            "algorithm": "default-memory-v2",
        },
    )

    def bind_user(self, user_id: str) -> AbstractContextManager[object]:
        store = self._require_v2_store()
        binder = getattr(store, "bind_user", None)
        if not callable(binder):
            raise RuntimeError("Cloud Memory store does not support user binding")
        return cast(AbstractContextManager[object], binder(user_id))


def build_cloud_memory_services(
    *,
    app_config: dict[str, Any],
    settings: CloudSettings,
    workspace: Path,
    provider: LLMProvider,
    light_provider: LLMProvider | None = None,
    http_resources: SharedHttpResources | None = None,
    event_publisher: Any = None,
    markdown: Any = None,
    sync_engine: Engine | None = None,
) -> MemoryServices:
    """Build the Cloud store while preserving DefaultMemoryEngine algorithms."""
    config = build_config(app_config)
    if not config.memory.enabled:
        raise RuntimeError("Cloud Memory cannot be disabled")
    if not config.memory.embedding.base_url:
        raise RuntimeError("Cloud Memory requires an embedding endpoint")
    if config.memory.embedding.output_dimensionality != VEC_DIM:
        raise RuntimeError(
            "Cloud Memory requires memory.embedding.output_dimensionality="
            f"{VEC_DIM} for the pgvector index"
        )
    resources = http_resources or SharedHttpResources()
    store = UserScopedPostgresMemoryStore(
        sync_engine or build_sync_engine(settings, pool_pre_ping=True),
        vector_dimension=VEC_DIM,
    )
    engine = CloudDefaultMemoryEngine(
        config=config,
        default_config=DefaultMemoryConfig(),
        workspace=workspace,
        provider=provider,
        light_provider=light_provider,
        http_resources=resources,
        event_publisher=event_publisher,
        memory_persistence=store,
    )
    return MemoryServices(engine=engine, store=store, markdown=markdown)
