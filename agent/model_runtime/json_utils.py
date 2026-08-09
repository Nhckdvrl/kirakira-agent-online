"""Loose JSON parsing helpers for model responses."""

from __future__ import annotations

from typing import Any

import json_repair


def strip_json_fence(text: str) -> str:
    payload = (text or "").strip()
    if payload.startswith("```"):
        payload = payload.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    return payload


def load_json_object_loose(text: str) -> dict[str, Any] | None:
    data = json_repair.loads(strip_json_fence(text))
    return data if isinstance(data, dict) else None
