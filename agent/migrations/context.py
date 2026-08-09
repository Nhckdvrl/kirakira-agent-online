from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class MigrationContext:
    config_path: Path
    workspace: Path


_CURRENT_CONTEXT: ContextVar[MigrationContext | None] = ContextVar(
    "kirakira_migration_context",
    default=None,
)


@contextmanager
def bind_migration_context(
    *,
    config_path: Path,
    workspace: Path,
) -> Iterator[MigrationContext]:
    """Expose installation paths while Yoyo invokes Python migration steps."""
    context = MigrationContext(config_path=config_path, workspace=workspace)
    token = _CURRENT_CONTEXT.set(context)
    try:
        yield context
    finally:
        _CURRENT_CONTEXT.reset(token)


def current_migration_context() -> MigrationContext:
    context = _CURRENT_CONTEXT.get()
    if context is None:
        raise RuntimeError("migration callback has no Kirakira installation context")
    return context
