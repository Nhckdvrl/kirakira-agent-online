from __future__ import annotations

import subprocess
from pathlib import Path

import scripts.check_yoyo_migrations as checker


def _git(repo: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    migration = repo / "migrations/yoyo/origin.py"
    migration.parent.mkdir(parents=True)
    migration.write_text("steps = []\n", encoding="utf-8")
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "baseline")
    return repo, _git(repo, "rev-parse", "HEAD")


def test_existing_migration_is_immutable(tmp_path: Path, monkeypatch) -> None:
    repo, base = _repository(tmp_path)
    migration = repo / "migrations/yoyo/origin.py"
    migration.write_text("steps = ['changed']\n", encoding="utf-8")
    monkeypatch.setattr(checker, "ROOT", repo)

    assert checker.violations(base) == [
        "registered Yoyo migration changed: migrations/yoyo/origin.py"
    ]


def test_new_migration_is_allowed(tmp_path: Path, monkeypatch) -> None:
    repo, base = _repository(tmp_path)
    (repo / "migrations/yoyo/next.py").write_text(
        "steps = []\n", encoding="utf-8"
    )
    _git(repo, "add", "migrations/yoyo/next.py")
    monkeypatch.setattr(checker, "ROOT", repo)

    assert checker.violations(base) == []
