"""Supervised gateway readiness marker, following Reference's boot ownership rule."""

from __future__ import annotations

import json
import os
from pathlib import Path


class RuntimeReadiness:
    def __init__(self, workspace: Path, boot_id: str) -> None:
        if not boot_id:
            raise ValueError("boot_id 不能为空")
        self.path = workspace / ".runtime-ready.json"
        self.boot_id = boot_id
        self.pid = os.getpid()

    def mark_ready(self) -> None:
        payload = {"bootId": self.boot_id, "pid": self.pid, "state": "ready"}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(
            f".{self.path.name}.{self.pid}.{self.boot_id}.tmp"
        )
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, self.path)

    def clear(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if payload.get("bootId") == self.boot_id and payload.get("pid") == self.pid:
            self.path.unlink()
