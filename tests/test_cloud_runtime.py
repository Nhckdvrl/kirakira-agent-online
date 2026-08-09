from __future__ import annotations

from dataclasses import dataclass

import pytest
from sqlalchemy import create_engine

from agent.tools.execution_backend import ExecutionBackendDescriptor
from agent.tools.unified_exec import ExecutionCleanupReport
from cloud.database import CloudSettings
from cloud.memory import CloudDefaultMemoryEngine
from cloud.models import Base
from cloud.runtime import build_cloud_worker_runtime


class _UnusedModelClient:
    pass


class _IsolatedBackend:
    descriptor = ExecutionBackendDescriptor(
        name="test-sandbox",
        isolated=True,
        host_execution=False,
        workspace_isolated=True,
    )

    async def probe(self) -> ExecutionBackendDescriptor:
        return self.descriptor

    async def shutdown(self) -> ExecutionCleanupReport:
        return ExecutionCleanupReport((), (), ())

    async def terminate_owner(self, owner_session_key: str) -> ExecutionCleanupReport:
        return ExecutionCleanupReport((), (), ())


@pytest.mark.asyncio
async def test_cloud_worker_composition_has_no_local_state_fallback(tmp_path) -> None:
    database_path = tmp_path / "cloud.db"
    sync_url = f"sqlite+pysqlite:///{database_path}"
    sync_engine = create_engine(sync_url)
    Base.metadata.create_all(sync_engine)
    sync_engine.dispose()
    settings = CloudSettings(
        database_url=f"sqlite+aiosqlite:///{database_path}",
        session_cookie_secure=False,
    )
    config = {
        "llm": {
            "main": {
                "model": "fake",
                "base_url": "https://model.example.test/v1",
                "context_window": 128_000,
            }
        },
        "memory": {
            "enabled": True,
            "embedding": {
                "model": "fake-embedding",
                "base_url": "https://embedding.example.test/v1",
                "output_dimensionality": 1024,
            },
        },
    }
    backend = _IsolatedBackend()
    runtime = await build_cloud_worker_runtime(
        workspace=tmp_path,
        app_config=config,
        settings=settings,
        execution_backend=backend,  # type: ignore[arg-type]
        worker_id="test-worker",
        model_client=_UnusedModelClient(),
    )
    try:
        assert isinstance(runtime.memory_services.engine, CloudDefaultMemoryEngine)
        assert runtime.memory_services.store is not None
        assert runtime.memory_services.markdown is runtime.markdown
        assert runtime.executor._scope_binders
        assert callable(runtime.executor._settle)
        handlers = runtime.tools._tools["bash"].handler.__self__
        assert handlers._execution_backend is backend
        assert handlers._workspace_backend is backend
    finally:
        await runtime.aclose()
