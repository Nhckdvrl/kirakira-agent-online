"""Kirakira Agent skill loader.

Frontmatter and availability semantics follow the akashic-agent reference
`SkillsLoader` (`Reference/agent/skills.py`): YAML frontmatter, a JSON-ish
`metadata` block whose `akashic`/`skill` object may declare `always` and
`requires` (missing `bins`/`env` mark the skill unavailable), and an optional
`when_to_use` hint. The public surface (`reload`, `names`, `always_names`,
`descriptions`, `load`) is kept stable for the rest of kirakira.
"""

import json
import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


class SkillLoader:
    def __init__(self, skills_dir: Path) -> None:
        self.skills_dir = skills_dir
        self._skills: Dict[str, Dict[str, Any]] = {}
        self.reload()

    def reload(self) -> None:
        self._skills = {}
        if not self.skills_dir.exists():
            return
        for path in sorted(self.skills_dir.rglob("SKILL.md")):
            text = path.read_text(encoding="utf-8")
            meta, body = self._parse(text)
            name = str(meta.get("name") or path.parent.name)
            config = self._parse_skill_config(meta.get("metadata"), path)
            missing = self._missing_requirements(config)
            self._skills[name] = {
                "name": name,
                "description": str(meta.get("description", "-")),
                "when_to_use": str(meta.get("when_to_use", "")),
                "body": body,
                "path": str(path),
                "always": self._as_bool(config.get("always"))
                or self._as_bool(meta.get("always", meta.get("always_on"))),
                "available": not missing,
                "missing": missing,
            }

    def names(self) -> List[str]:
        return sorted(self._skills.keys())

    def always_names(self) -> List[str]:
        """Return available skills whose frontmatter opts into every context frame."""

        return sorted(
            name
            for name, skill in self._skills.items()
            if skill.get("always") and skill.get("available", True)
        )

    def descriptions(self) -> str:
        if not self._skills:
            return "(no skills)"
        lines: List[str] = []
        for name in self.names():
            skill = self._skills[name]
            line = " - %s: %s" % (name, skill.get("description", "-"))
            when = skill.get("when_to_use")
            if when:
                line += " [when: %s]" % when
            if not skill.get("available", True):
                line += " [unavailable: %s]" % skill.get("missing", "requirements")
            lines.append(line)
        return "\n".join(lines)

    def load(self, name: str) -> str:
        skill = self._skills.get(name)
        if skill is None:
            available = ", ".join(self.names()) or "(none)"
            return "Error: Unknown skill '%s'. Available: %s" % (name, available)
        if not skill.get("available", True):
            return "Error: skill '%s' is unavailable (%s)." % (
                name,
                skill.get("missing", "missing requirements"),
            )
        return '<skill name="%s">\n%s\n</skill>' % (name, skill["body"])

    # ----------------------------------------------------------------- parsing
    def _parse(self, text: str) -> Tuple[Dict[str, Any], str]:
        if not text.startswith("---"):
            return {}, text.strip()
        match = re.match(r"^---\n(.*?)\n---\n?(.*)", text, re.DOTALL)
        if not match:
            return {}, text.strip()
        loaded: Any = yaml.safe_load(match.group(1)) or {}
        if not isinstance(loaded, dict):
            return {}, match.group(2).strip()
        meta = {str(key): value for key, value in loaded.items()}
        return meta, match.group(2).strip()

    def _parse_skill_config(self, raw: Any, skill_file: Path) -> Dict[str, Any]:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            data: Dict[str, Any] = dict(raw)
        else:
            text = str(raw).strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return {}
            if not isinstance(parsed, dict):
                return {}
            data = parsed
        for key in ("akashic", "skill"):
            value = data.get(key)
            if isinstance(value, dict):
                return value
        return data

    def _missing_requirements(self, config: Dict[str, Any]) -> str:
        requires = config.get("requires", {})
        if not isinstance(requires, dict):
            return ""
        missing: List[str] = []
        for binary in self._string_list(requires.get("bins")):
            if not shutil.which(binary):
                missing.append("CLI: %s" % binary)
        for env in self._string_list(requires.get("env")):
            if not os.environ.get(env):
                missing.append("ENV: %s" % env)
        return ", ".join(missing)

    @staticmethod
    def _string_list(value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [item for item in value if isinstance(item, str)]

    @staticmethod
    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in {"1", "true", "yes", "on"}
        return False


def list_skills(skills_dir: Path) -> str:
    return SkillLoader(skills_dir).descriptions()
