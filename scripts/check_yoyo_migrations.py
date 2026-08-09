#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PREFIX = "migrations/yoyo/"


def _git(*arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Git command failed: git {' '.join(arguments)}: {result.stderr.strip()}"
        )
    return result.stdout


def violations(base: str) -> list[str]:
    """Reject edits, moves, and deletion of migrations registered at the base."""
    base_paths = {
        path
        for path in _git(
            "ls-tree", "-r", "--name-only", base, "--", "migrations/yoyo"
        ).splitlines()
        if path.startswith(CATALOG_PREFIX) and path.endswith(".py")
    }
    problems: list[str] = []
    changes = _git(
        "diff",
        "--name-status",
        "--find-renames",
        base,
        "--",
        "migrations/yoyo",
    )
    for line in changes.splitlines():
        fields = line.split("\t")
        status, paths = fields[0], fields[1:]
        if status == "A":
            continue
        immutable = sorted(base_paths.intersection(paths))
        if immutable:
            problems.append(f"registered Yoyo migration changed: {immutable[0]}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    args = parser.parse_args()
    problems = violations(str(args.base))
    for problem in problems:
        print(problem)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
