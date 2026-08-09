"""Declarative workspace MCP specs.

非插件 MCP 由 `workspace/mcp/servers/*.toml` 声明，一个文件一个 server，文件名必须与
`name` 一致。运行时只按内容 revision 决定是否换代；任何一份声明非法都会让整批候选被
拒绝，旧代际继续服务。
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

_ALLOWED_FIELDS = {"schema_version", "name", "command", "cwd", "env", "watch_paths"}


@dataclass(frozen=True)
class WorkspaceMcpDeclarations:
    specs: Dict[str, Dict[str, Any]]
    revision: str
    watched_files: Tuple[Path, ...]


def load_workspace_mcp_declarations(
    root: Path,
    *,
    mcp_root: Path | None = None,
) -> WorkspaceMcpDeclarations:
    """严格解析 workspace MCP 声明并计算内容 revision。"""

    # 1. 解析并校验每个独立 server 声明。
    root = root.resolve(strict=False)
    safe_root = (mcp_root or root.parent).resolve(strict=False)
    if not root.is_relative_to(safe_root):
        raise ValueError("MCP declarations dir escapes safe root: %s" % root)
    specs: Dict[str, Dict[str, Any]] = {}
    watched: set[Path] = set()
    normalized: List[Dict[str, object]] = []
    for path in sorted(root.glob("*.toml")) if root.is_dir() else ():
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        if set(raw) - _ALLOWED_FIELDS:
            raise ValueError("MCP declaration has unknown fields: %s" % path)
        name = raw.get("name")
        schema_version = raw.get("schema_version")
        if (
            isinstance(schema_version, bool)
            or schema_version != 1
            or not isinstance(name, str)
            or name != path.stem
        ):
            raise ValueError("MCP declaration schema_version/name invalid: %s" % path)
        if name in specs:
            raise ValueError("duplicate MCP server name: %s" % name)
        command = raw.get("command")
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(item, str) and item for item in command)
        ):
            raise ValueError("MCP command invalid: %s" % path)
        env = raw.get("env", {})
        if not isinstance(env, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in env.items()
        ):
            raise ValueError("MCP env invalid: %s" % path)
        base = path.parent.resolve()
        cwd = _resolve_inside(base, safe_root, raw.get("cwd"), field="cwd", path=path)
        watch_raw = raw.get("watch_paths", [])
        if not isinstance(watch_raw, list) or not all(
            isinstance(item, str) and item for item in watch_raw
        ):
            raise ValueError("MCP watch_paths invalid: %s" % path)
        watch_paths = tuple(
            _resolve_inside(base, safe_root, item, field="watch_paths", path=path)
            for item in watch_raw
        )
        watched.update(path for path in watch_paths if path is not None)
        spec: Dict[str, Any] = {"command": list(command), "env": dict(env)}
        if cwd is not None:
            spec["cwd"] = str(cwd)
        specs[name] = spec
        normalized.append(
            {"name": name, **spec, "watch_paths": [str(p) for p in watch_paths]}
        )

    # 2. revision 只取规范化声明和 watch 文件内容，与文件 mtime 无关。
    digest = hashlib.sha256(
        json.dumps(normalized, ensure_ascii=False, sort_keys=True).encode("utf-8")
    )
    for path in sorted(watched):
        _hash_watch_path(digest, path)
    return WorkspaceMcpDeclarations(specs, digest.hexdigest(), tuple(sorted(watched)))


def declarations_input_revision(
    root: Path,
    *,
    mcp_root: Path | None = None,
) -> str:
    """计算声明文件与当前有效 watch paths 的轮询输入指纹。"""

    digest = hashlib.sha256()
    root = root.resolve(strict=False)
    for path in sorted(root.glob("*.toml")) if root.is_dir() else ():
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    try:
        desired = load_workspace_mcp_declarations(root, mcp_root=mcp_root)
    except (OSError, ValueError):
        # 声明当前非法时，指纹只覆盖原始文件；修好后自然产生新指纹并重试。
        return digest.hexdigest()
    for path in desired.watched_files:
        _hash_watch_path(digest, path)
    return digest.hexdigest()


def _hash_watch_path(digest: Any, path: Path) -> None:
    if path.is_file():
        _hash_watch_entry(digest, b"file", str(path).encode("utf-8"), path.read_bytes())
        return
    if path.is_dir():
        _hash_watch_entry(digest, b"directory", str(path).encode("utf-8"), b"")
        for child in sorted(item for item in path.rglob("*") if item.is_file()):
            _hash_watch_entry(
                digest,
                b"child",
                str(child.relative_to(path)).encode("utf-8"),
                child.read_bytes(),
            )
        return
    _hash_watch_entry(digest, b"missing", str(path).encode("utf-8"), b"")


def _hash_watch_entry(
    digest: Any,
    entry_type: bytes,
    entry_path: bytes,
    content: bytes,
) -> None:
    """以长度分帧编码 watch entry，避免路径与内容拼接碰撞。"""

    digest.update(len(entry_type).to_bytes(4, "big"))
    digest.update(entry_type)
    digest.update(len(entry_path).to_bytes(8, "big"))
    digest.update(entry_path)
    digest.update(len(content).to_bytes(8, "big"))
    digest.update(content)


def _resolve_inside(
    base: Path,
    safe_root: Path,
    raw: object,
    *,
    field: str,
    path: Path,
) -> Path | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw:
        raise ValueError("MCP %s invalid: %s" % (field, path))
    resolved = (base / raw).resolve(strict=False)
    if not resolved.is_relative_to(safe_root):
        raise ValueError("MCP %s escapes safe root: %s" % (field, path))
    return resolved
