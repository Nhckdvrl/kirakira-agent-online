"""Register the current Kirakira workspace schema without rewriting user data."""

from __future__ import annotations

from yoyo import step

from agent.migrations.context import current_migration_context


__depends__: set[str] = set()


def record_current_workspace_origin(_connection: object) -> None:
    """Record the existing schema as origin; Akasha v1 and other data stay untouched."""
    context = current_migration_context()
    context.workspace.mkdir(parents=True, exist_ok=True)


steps = [step(record_current_workspace_origin)]
