"""Atomic filesystem persistence primitives."""

from __future__ import annotations

import os
import secrets
from pathlib import Path

_TEMP_ATTEMPTS = 8


def atomic_write_text(path: Path, content: str, *, domain: str = "json_store") -> None:
    """Atomically replace UTF-8 text and durably publish the directory entry."""
    del domain
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    for _attempt in range(_TEMP_ATTEMPTS):
        candidate = path.parent / ("%s.%s.tmp" % (path.name, secrets.token_hex(16)))
        try:
            handle = os.open(candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            continue
        temporary = candidate
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            temporary = None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return
    raise OSError("unable to create atomic-write temporary file: %s" % path)
