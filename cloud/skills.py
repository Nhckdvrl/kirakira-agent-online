"""Validation and task-local projection for durable user Skills."""

from __future__ import annotations

import re
from typing import Any

import yaml


def parse_skill_document(content: str) -> dict[str, Any]:
    text = content.strip()
    if not text or len(text) > 1_000_000:
        raise ValueError("skill document must contain 1 to 1,000,000 characters")
    meta: dict[str, Any] = {}
    body = text
    if text.startswith("---"):
        match = re.match(r"^---\n(.*?)\n---\n?(.*)", text, re.DOTALL)
        if not match:
            raise ValueError("skill YAML frontmatter is malformed")
        loaded = yaml.safe_load(match.group(1)) or {}
        if not isinstance(loaded, dict):
            raise ValueError("skill frontmatter must be an object")
        meta = dict(loaded)
        body = match.group(2).strip()
    name = str(meta.get("name") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_:-]{1,100}", name):
        raise ValueError("skill frontmatter requires a valid name")
    if not body:
        raise ValueError("skill body must not be empty")
    metadata = meta.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = yaml.safe_load(metadata) or {}
        except yaml.YAMLError:
            metadata = {}
    config = metadata.get("skill", metadata.get("akashic", metadata)) if isinstance(metadata, dict) else {}
    always = bool(meta.get("always", meta.get("always_on", False)))
    if isinstance(config, dict):
        always = always or bool(config.get("always", False))
    return {
        "name": name,
        "description": str(meta.get("description") or "-")[:1000],
        "when_to_use": str(meta.get("when_to_use") or "")[:2000],
        "body": body,
        "always": always,
    }


def skill_overlay(rows: list[Any]) -> dict[str, dict[str, Any]]:
    return {
        row.name: {
            "name": row.name,
            "description": row.description,
            "when_to_use": row.when_to_use,
            "body": row.body,
            "path": f"cloud://skills/{row.id}",
            "always": row.always,
            "available": True,
            "missing": "",
        }
        for row in rows
    }
