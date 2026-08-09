"""Cloud production composition must fail closed on Local or reduced dependencies."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from cloud.readiness import (
    CloudWorkerNotReadyError,
    assess_cloud_worker_readiness,
)
from cloud.transcript import RunScopedTranscriptStore
from core.memory.engine import (
    EngineProfile,
    MemoryCapability,
    MemoryEngineDescriptor,
)
from core.memory.plugin import DisabledMemoryEngine
from session.manager import SessionManager


class MemoryServices:
    def __init__(self, engine, markdown=None) -> None:
        self.engine = engine
        self.markdown = markdown


class CloudMemoryEngine:
    def describe(self):
        return MemoryEngineDescriptor(
            name="cloud-default",
            profile=EngineProfile.RICH_MEMORY_ENGINE,
            capabilities=frozenset(
                {
                    MemoryCapability.INGEST_MESSAGES,
                    MemoryCapability.RETRIEVE_CONTEXT_BLOCK,
                    MemoryCapability.RETRIEVE_STRUCTURED_HITS,
                    MemoryCapability.MANAGE_HISTORY,
                    MemoryCapability.MANAGE_UPDATE,
                    MemoryCapability.MANAGE_DELETE,
                    MemoryCapability.SEMANTICS_RICH_MEMORY,
                }
            ),
            notes={"durable_backend": "postgresql", "tenant_scope": "user"},
        )


@dataclass(frozen=True)
class ExecutionDescriptor:
    isolated: bool
    host_execution: bool
    workspace_isolated: bool


class ExecutionBackend:
    descriptor = ExecutionDescriptor(
        isolated=True, host_execution=False, workspace_isolated=True
    )


def test_local_and_disabled_dependencies_are_rejected(tmp_path):
    local_sessions = SessionManager(tmp_path)
    try:
        readiness = assess_cloud_worker_readiness(
            transcript_store=local_sessions,
            memory_services=MemoryServices(DisabledMemoryEngine()),
            execution_backend=None,
        )
    finally:
        local_sessions.close()

    assert not readiness.ready
    assert any("RunScopedTranscriptStore" in item for item in readiness.blockers)
    assert any("algorithm capabilities" in item for item in readiness.blockers)
    assert any("ExecutionBackend" in item for item in readiness.blockers)
    with pytest.raises(CloudWorkerNotReadyError):
        readiness.require_ready()


def test_full_cloud_capability_evidence_passes():
    markdown = type(
        "CloudMarkdown",
        (),
        {"descriptor": {"durable_backend": "postgresql", "tenant_scope": "user"}},
    )()
    readiness = assess_cloud_worker_readiness(
        transcript_store=RunScopedTranscriptStore(),
        memory_services=MemoryServices(CloudMemoryEngine(), markdown),
        execution_backend=ExecutionBackend(),
    )

    assert readiness.ready
    readiness.require_ready()
