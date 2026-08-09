"""Agent-managed workspace MCP declarations.

Agent 通过 `apply` / `remove` 增删 `workspace/mcp/servers/*.toml` 声明，走的仍是
watcher 那条 reconcile 路径：写完声明立刻发布一代新的 MCP catalog，失败则把文件恢复
原样、旧代际继续服务。声明文件是人手改与 agent 改共享的同一份真相，所以渲染出的 TOML
必须能被严格的声明加载器接受。

`status` 只读地汇报每份声明的状态。它可以把 env 的**键名**给模型看，但绝不泄露值。
"""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from agent.mcp.watcher import WorkspaceMcpWatcher
from agent.tools.registry import ToolRegistry, object_schema
from core.schema import ToolSpec

_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*$")


class WorkspaceMcpAdmin:
    """让 agent 声明式地增删 workspace MCP server。"""

    def __init__(self, workspace: Path, watcher: WorkspaceMcpWatcher) -> None:
        self._workspace = Path(workspace)
        self._watcher = watcher
        self._servers_dir = self._workspace / "mcp" / "servers"

    # ------------------------------------------------------------------ apply
    async def apply(
        self,
        *,
        name: str,
        command: Sequence[str],
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
        watch_paths: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """写入并立即发布一份声明；起不来就回滚，旧代际不受影响。"""

        self._validate_name(name)
        self._validate_command(command)
        env_map = self._validate_env(env)
        watch_list = self._validate_watch_paths(watch_paths)

        self._servers_dir.mkdir(parents=True, exist_ok=True)
        path = self._servers_dir / ("%s.toml" % name)
        previous = path.read_text(encoding="utf-8") if path.is_file() else None
        rendered = self._render(
            name=name,
            command=list(command),
            env=env_map,
            cwd=cwd,
            watch_paths=watch_list,
        )

        _atomic_write(path, rendered)
        try:
            # 写完立刻走 reconcile：任一 server 连不上都会抛，旧快照继续服务。
            await self._watcher.reconcile()
        except Exception as error:  # noqa: BLE001 - 统一回滚后再抛出
            self._restore(path, previous)
            raise RuntimeError(
                "MCP server '%s' failed to start; declaration rolled back: %s"
                % (name, error)
            ) from error

        return {"status": "active", "effectiveFrom": "next_turn"}

    # ----------------------------------------------------------------- remove
    async def remove(self, name: str) -> Dict[str, Any]:
        """删除声明并排空对应 server；删除前留一份内容备份。"""

        self._validate_name(name)
        path = self._servers_dir / ("%s.toml" % name)
        if not path.is_file():
            raise ValueError("unknown MCP server declaration: %s" % name)

        original = path.read_text(encoding="utf-8")
        backup = self._servers_dir / ("%s.toml.removed.bak" % name)
        _atomic_write(backup, original)
        path.unlink()
        try:
            await self._watcher.reconcile()
        except Exception as error:  # noqa: BLE001 - 恢复文件后再抛出
            self._restore(path, original)
            raise RuntimeError(
                "removing MCP server '%s' failed; declaration restored: %s"
                % (name, error)
            ) from error

        return {"status": "removed", "backup": str(backup)}

    # ----------------------------------------------------------------- status
    def status(self) -> Dict[str, Any]:
        """逐份声明的只读状态。env 只露键名，绝不露值。"""

        declarations: List[Dict[str, Any]] = []
        if self._servers_dir.is_dir():
            for path in sorted(self._servers_dir.glob("*.toml")):
                declarations.append(self._describe(path))
        return {
            "declarations": declarations,
            "runtime": self._watcher.status(),
        }

    # -------------------------------------------------------------- tool glue
    def register_tools(self, registry: ToolRegistry) -> None:
        registry.register(
            ToolSpec(
                "mcp_apply",
                "Declare or update a workspace MCP server (writes mcp/servers/<name>.toml "
                "and hot-reloads it). Rolls the declaration back if the server cannot start.",
                object_schema(
                    {
                        "name": {"type": "string"},
                        "command": {"type": "array", "items": {"type": "string"}},
                        "env": {"type": "object"},
                        "cwd": {"type": "string"},
                        "watch_paths": {"type": "array", "items": {"type": "string"}},
                    },
                    ["name", "command"],
                ),
            ),
            self._tool_apply,
            deferred=True,
        )
        registry.register(
            ToolSpec(
                "mcp_remove",
                "Remove a workspace MCP server declaration and drain its process.",
                object_schema({"name": {"type": "string"}}, ["name"]),
            ),
            self._tool_remove,
            deferred=True,
        )
        registry.register(
            ToolSpec(
                "mcp_status",
                "Report the state of every workspace MCP declaration (env keys only, never values).",
                object_schema({}, []),
            ),
            self._tool_status,
            deferred=True,
        )

    async def _tool_apply(
        self,
        *,
        name: str,
        command: Sequence[str],
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
        watch_paths: Optional[Sequence[str]] = None,
    ) -> str:
        result = await self.apply(
            name=name, command=command, env=env, cwd=cwd, watch_paths=watch_paths
        )
        return json.dumps(result, ensure_ascii=False)

    async def _tool_remove(self, *, name: str) -> str:
        return json.dumps(await self.remove(name), ensure_ascii=False)

    def _tool_status(self) -> str:
        return json.dumps(self.status(), ensure_ascii=False)

    # ------------------------------------------------------------- validation
    @staticmethod
    def _validate_name(name: object) -> None:
        if not isinstance(name, str) or not _NAME_RE.match(name):
            raise ValueError(
                "MCP server name must match [a-z][a-z0-9_-]*: %r" % (name,)
            )

    @staticmethod
    def _validate_command(command: object) -> None:
        if (
            not isinstance(command, (list, tuple))
            or not command
            or not all(isinstance(item, str) and item for item in command)
        ):
            raise ValueError("MCP command must be a non-empty list of strings")

    @staticmethod
    def _validate_env(env: object) -> Dict[str, str]:
        if env is None:
            return {}
        if not isinstance(env, Mapping) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in env.items()
        ):
            raise ValueError("MCP env must be a mapping of string keys to string values")
        return dict(env)

    @staticmethod
    def _validate_watch_paths(watch_paths: object) -> List[str]:
        if watch_paths is None:
            return []
        if not isinstance(watch_paths, (list, tuple)) or not all(
            isinstance(item, str) and item for item in watch_paths
        ):
            raise ValueError("MCP watch_paths must be a list of non-empty strings")
        return list(watch_paths)

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _render(
        *,
        name: str,
        command: List[str],
        env: Dict[str, str],
        cwd: Optional[str],
        watch_paths: List[str],
    ) -> str:
        lines = [
            "schema_version = 1",
            "name = %s" % _toml_str(name),
            "command = %s" % _toml_array(command),
        ]
        if cwd is not None:
            lines.append("cwd = %s" % _toml_str(cwd))
        if watch_paths:
            lines.append("watch_paths = %s" % _toml_array(watch_paths))
        if env:
            inline = ", ".join(
                "%s = %s" % (_toml_str(key), _toml_str(value))
                for key, value in env.items()
            )
            lines.append("env = { %s }" % inline)
        return "\n".join(lines) + "\n"

    @staticmethod
    def _restore(path: Path, previous: Optional[str]) -> None:
        if previous is None:
            if path.exists():
                path.unlink()
        else:
            _atomic_write(path, previous)

    def _describe(self, path: Path) -> Dict[str, Any]:
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            return {"name": path.stem, "state": "invalid", "envKeys": [], "error": str(error)}
        error = self._declaration_error(path.stem, raw)
        env = raw.get("env", {})
        env_keys = list(env) if isinstance(env, dict) else []
        if error is not None:
            return {"name": path.stem, "state": "invalid", "envKeys": env_keys, "error": error}
        return {"name": path.stem, "state": "active", "envKeys": env_keys, "error": None}

    @staticmethod
    def _declaration_error(stem: str, raw: Mapping[str, Any]) -> Optional[str]:
        schema_version = raw.get("schema_version")
        name = raw.get("name")
        if isinstance(schema_version, bool) or schema_version != 1:
            return "schema_version must be 1"
        if not isinstance(name, str) or name != stem:
            return "name must match the file name"
        command = raw.get("command")
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(item, str) and item for item in command)
        ):
            return "command must be a non-empty list of strings"
        env = raw.get("env", {})
        if not isinstance(env, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in env.items()
        ):
            return "env must be a mapping of string keys to string values"
        return None


def _toml_str(value: str) -> str:
    # TOML basic strings share JSON 的转义规则；json.dumps 处理引号/反斜杠/控制字符。
    return json.dumps(value, ensure_ascii=False)


def _toml_array(values: Sequence[str]) -> str:
    return "[%s]" % ", ".join(_toml_str(item) for item in values)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)
