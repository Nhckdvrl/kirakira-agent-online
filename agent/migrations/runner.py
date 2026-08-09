from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal
from urllib.parse import quote

from yoyo import get_backend, read_migrations

from agent.migrations.context import bind_migration_context
from bootstrap.workspace_lock import WorkspaceInstanceLock


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_USERNAME_ENV_KEYS = ("LOGNAME", "USER", "LNAME", "USERNAME")


@dataclass(frozen=True)
class MigrationOutcome:
    state: Literal["current", "migrated"]
    migrations: tuple[str, ...] = ()


class MigrationRunner:
    """Apply pending, version-controlled workspace migrations before startup."""

    def __init__(
        self,
        *,
        repo_root: Path,
        config_path: Path,
        workspace: Path,
    ) -> None:
        self.repo_root = repo_root.resolve()
        self.config_path = config_path.expanduser().resolve()
        self.workspace = workspace.expanduser().resolve()
        self.migrations_root = self.repo_root / "migrations" / "yoyo"
        self.ledger_path = self.workspace / "migrations.sqlite3"

    def run(self) -> MigrationOutcome:
        workspace_lock = WorkspaceInstanceLock(self.workspace)
        workspace_lock.acquire()
        try:
            return self._apply_pending()
        finally:
            workspace_lock.release()

    def _apply_pending(self) -> MigrationOutcome:
        migration_ids: tuple[str, ...] = ()
        try:
            if not self.migrations_root.is_dir():
                raise FileNotFoundError(
                    f"migration catalog does not exist: {self.migrations_root}"
                )
            self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
            migrations = read_migrations(str(self.migrations_root))
            backend = get_backend(self._ledger_uri())
            os.chmod(self.ledger_path, 0o600)
            with (
                _bind_yoyo_username(),
                backend,
                bind_migration_context(
                    config_path=self.config_path,
                    workspace=self.workspace,
                ),
            ):
                pending = backend.to_apply(migrations)
                migration_ids = tuple(migration.id for migration in pending)
                backend.apply_migrations(pending)
        except Exception as exc:
            raise RuntimeError(
                f"Yoyo migration failed: ledger={self.ledger_path} detail={exc}"
            ) from exc

        state: Literal["current", "migrated"] = (
            "migrated" if migration_ids else "current"
        )
        return MigrationOutcome(state=state, migrations=migration_ids)

    def _ledger_uri(self) -> str:
        encoded = quote(self.ledger_path.as_posix(), safe="/:")
        return f"sqlite:///{encoded}"


@contextmanager
def _bind_yoyo_username() -> Iterator[None]:
    """Provide a stable audit identity in containers without an OS user."""
    if any(os.environ.get(key) for key in _USERNAME_ENV_KEYS):
        yield
        return

    os.environ["USER"] = "kirakira"
    try:
        yield
    finally:
        del os.environ["USER"]


def migrate_installation(config_path: Path, workspace: Path) -> MigrationOutcome:
    return MigrationRunner(
        repo_root=_PROJECT_ROOT,
        config_path=config_path,
        workspace=workspace,
    ).run()
