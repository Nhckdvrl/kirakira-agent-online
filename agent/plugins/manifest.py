"""Plugin discovery and the global enable/disable manifest.

全局清单只回答“插件是否启用”。能力（skills、MCP、工具、phase 模块）一律由插件根目录的
`plugin.py` 用代码声明，不再有 `.aka-plugin/plugin.json` 这类描述符文件。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

try:
    import tomllib
except ImportError:  # pragma: no cover - Python 3.10 fallback
    tomllib = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.toml"
PLUGIN_ENTRY = "plugin.py"
_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._@-]*")


@dataclass(frozen=True)
class PluginEnablement:
    """全局清单里单个插件的启停状态。"""

    plugin_id: str
    enabled: bool


def load_manifest(path: Path) -> Dict[str, PluginEnablement]:
    """严格解析全局启停清单；清单损坏时直接失败，不静默当作全部启用。"""

    if not path.exists():
        return {}
    if tomllib is None:
        raise RuntimeError("plugin manifest requires Python 3.11+")
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    if set(raw) - {"plugins"}:
        raise ValueError("plugin manifest has unknown top-level fields: %s" % path)
    plugins = raw.get("plugins", {})
    if not isinstance(plugins, dict):
        raise ValueError("plugin manifest [plugins] must be a table: %s" % path)
    result: Dict[str, PluginEnablement] = {}
    for plugin_id, entry in plugins.items():
        if not _ID_PATTERN.fullmatch(plugin_id):
            raise ValueError("invalid plugin id in manifest: %r" % plugin_id)
        if not isinstance(entry, dict) or set(entry) - {"enabled"}:
            raise ValueError("plugin manifest entry invalid: %s" % plugin_id)
        enabled = entry.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("plugin manifest enabled must be a boolean: %s" % plugin_id)
        result[plugin_id] = PluginEnablement(plugin_id, enabled)
    return result


def is_enabled(manifest: Dict[str, PluginEnablement], plugin_id: str) -> bool:
    """清单未记录的插件默认启用，记录了就以清单为准。"""

    entry = manifest.get(plugin_id)
    return True if entry is None else entry.enabled


def discover_plugin_roots(plugin_dirs: List[Path]) -> List[Path]:
    """插件根目录的唯一标志是根目录下的 plugin.py。"""

    roots: List[Path] = []
    seen: set[Path] = set()
    for declared in plugin_dirs:
        candidates = [declared] if _is_plugin_root(declared) else []
        if declared.is_dir() and not candidates:
            candidates = [
                child for child in sorted(declared.iterdir()) if _is_plugin_root(child)
            ]
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved not in seen:
                seen.add(resolved)
                roots.append(resolved)
    return roots


def resolve_skill_roots(root: Path, declared: Any) -> tuple[Path, ...]:
    """把插件声明的 skill 目录解析到插件根内；声明了就必须存在。"""

    resolved: List[Path] = []
    for item in _strings(declared):
        path = safe_child(root, item)
        if not path.is_dir():
            raise ValueError("declared skill root does not exist: %s" % path)
        resolved.append(path)
    return tuple(resolved)


def safe_child(root: Path, value: str) -> Path:
    target = (root / value).resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError("plugin path escapes root: %s" % value) from exc
    return target


def normalize_command_item(root: Path, value: str) -> str:
    """命令里的相对路径按插件根解析，裸命令名保持原样交给 PATH 查找。"""

    path = Path(value).expanduser()
    if path.is_absolute():
        return str(path)
    if "/" not in value and "\\" not in value and not value.startswith("."):
        return value
    return str(safe_child(root, value))


def _is_plugin_root(path: Path) -> bool:
    return path.is_dir() and (path / PLUGIN_ENTRY).is_file()


def _strings(value: object) -> List[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, (list, tuple)):
        return []
    return [
        str(item).strip()
        for item in value
        if isinstance(item, str) and item.strip()
    ]
