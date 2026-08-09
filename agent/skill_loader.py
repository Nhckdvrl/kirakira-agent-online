"""Lightweight skill metadata loader used by memory tagging."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SkillRecord:
    name: str


class SkillsLoader:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace

    def list_skill_records(self, *, filter_unavailable: bool = False) -> list[SkillRecord]:
        del filter_unavailable
        root = self.workspace / "skills"
        if not root.exists():
            return []
        return [
            SkillRecord(path.parent.name)
            for path in sorted(root.glob("*/SKILL.md"))
        ]
