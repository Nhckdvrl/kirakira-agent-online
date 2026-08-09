"""Fail-closed production readiness checks for the Cloud Agent worker."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cloud.transcript import RunScopedTranscriptStore
from core.memory.engine import MemoryCapability


_REQUIRED_MEMORY_CAPABILITIES = frozenset(
    {
        MemoryCapability.INGEST_MESSAGES,
        MemoryCapability.RETRIEVE_CONTEXT_BLOCK,
        MemoryCapability.RETRIEVE_STRUCTURED_HITS,
        MemoryCapability.MANAGE_HISTORY,
        MemoryCapability.MANAGE_UPDATE,
        MemoryCapability.MANAGE_DELETE,
        MemoryCapability.SEMANTICS_RICH_MEMORY,
    }
)


class CloudWorkerNotReadyError(RuntimeError):
    pass


@dataclass(frozen=True)
class CloudWorkerReadiness:
    blockers: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.blockers

    def require_ready(self) -> None:
        if self.blockers:
            raise CloudWorkerNotReadyError(
                "Cloud Agent worker is not production-ready: " + "; ".join(self.blockers)
            )


def assess_cloud_worker_readiness(
    *,
    transcript_store: Any,
    memory_services: Any = None,
    execution_backend: Any = None,
) -> CloudWorkerReadiness:
    """Reject Local/no-op dependencies instead of silently degrading Agent behavior."""
    blockers: list[str] = []
    if not isinstance(transcript_store, RunScopedTranscriptStore):
        blockers.append("RunScopedTranscriptStore is required")

    engine = getattr(memory_services, "engine", None)
    descriptor = _memory_descriptor(engine)
    notes = dict(getattr(descriptor, "notes", {}) or {})
    capabilities = frozenset(getattr(descriptor, "capabilities", frozenset()) or ())
    missing = _REQUIRED_MEMORY_CAPABILITIES - capabilities
    if engine is None or descriptor is None:
        blockers.append("a Cloud MemoryEngine is required")
    else:
        if notes.get("durable_backend") != "postgresql":
            blockers.append("MemoryEngine durable_backend must be postgresql")
        if notes.get("tenant_scope") != "user":
            blockers.append("MemoryEngine tenant_scope must be user")
        if missing:
            blockers.append(
                "MemoryEngine is missing algorithm capabilities: "
                + ", ".join(sorted(item.value for item in missing))
            )

    markdown = getattr(memory_services, "markdown", None)
    markdown_descriptor = dict(getattr(markdown, "descriptor", {}) or {})
    if markdown is None:
        blockers.append("durable Cloud profile/compaction memory is required")
    else:
        if markdown_descriptor.get("durable_backend") != "postgresql":
            blockers.append("profile/compaction durable_backend must be postgresql")
        if markdown_descriptor.get("tenant_scope") != "user":
            blockers.append("profile/compaction tenant_scope must be user")

    execution_descriptor = getattr(execution_backend, "descriptor", None)
    if execution_descriptor is None:
        blockers.append("an isolated ExecutionBackend is required")
    else:
        if not bool(getattr(execution_descriptor, "isolated", False)):
            blockers.append("ExecutionBackend must be isolated")
        if bool(getattr(execution_descriptor, "host_execution", True)):
            blockers.append("ExecutionBackend must forbid host execution")
        if not bool(getattr(execution_descriptor, "workspace_isolated", False)):
            blockers.append("ExecutionBackend must isolate workspace storage")

    return CloudWorkerReadiness(tuple(blockers))


def _memory_descriptor(engine: Any) -> Any:
    describe = getattr(engine, "describe", None)
    if callable(describe):
        return describe()
    return getattr(engine, "DESCRIPTOR", None)
