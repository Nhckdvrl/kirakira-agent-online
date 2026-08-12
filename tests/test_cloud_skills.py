from __future__ import annotations

from pathlib import Path

from agent.skills import SkillLoader, bind_cloud_skills
from cloud.skills import parse_skill_document


def test_cloud_skill_preserves_catalog_always_mentions_and_load_tool_semantics(tmp_path: Path):
    parsed = parse_skill_document(
        """---
name: tenant-research
description: Tenant research workflow
when_to_use: when evidence is needed
always: true
---
Use two independent primary sources.
"""
    )
    loader = SkillLoader(tmp_path / "skills")
    record = {
        **parsed,
        "available": True,
        "missing": "",
        "path": "cloud://skills/1",
    }
    assert loader.names() == []
    with bind_cloud_skills({parsed["name"]: record}):
        assert loader.names() == ["tenant-research"]
        assert loader.always_names() == ["tenant-research"]
        assert "when evidence is needed" in loader.descriptions()
        assert "Use two independent" in loader.load("tenant-research")
    assert loader.names() == []


def test_skill_document_rejects_missing_frontmatter_name():
    try:
        parse_skill_document("plain body")
    except ValueError as exc:
        assert "valid name" in str(exc)
    else:
        raise AssertionError("missing skill name was accepted")
