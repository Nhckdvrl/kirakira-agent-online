"""Concurrent JSON-RPC client for one stdio MCP server."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class McpToolInfo:
    name: str
    description: str
    input_schema: Dict[str, Any]


class McpClient:
    def __init__(
        self,
        name: str,
        command: List[str],
        *,
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        connect_timeout: float = 10.0,
        request_timeout: float = 30.0,
    ) -> None:
        if not command:
            raise ValueError("MCP command cannot be empty")
        self.name = name
        self.command = list(command)
        self.env = dict(env or {})
        self.cwd = cwd or self._infer_cwd(command)
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.request_timeout = max(1.0, float(request_timeout))
        self.tool_infos: List[McpToolInfo] = []
        self._process: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._stderr_task: Optional[asyncio.Task[None]] = None
        self._pending: Dict[int, asyncio.Future[Dict[str, Any]]] = {}
        self._next_id = 1
        self._send_lock = asyncio.Lock()
        self._recent_stdout: deque[str] = deque(maxlen=8)
        self._recent_stderr: deque[str] = deque(maxlen=8)

    async def connect(self) -> List[McpToolInfo]:
        if self._process is not None:
            return list(self.tool_infos)
        try:
            await asyncio.wait_for(self._connect(), timeout=self.connect_timeout)
        except Exception:
            await self.disconnect()
            raise
        return list(self.tool_infos)

    async def _connect(self) -> None:
        self._process = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.cwd,
            env={**os.environ, **self.env},
            limit=4 * 1024 * 1024,
        )
        self._reader_task = asyncio.create_task(
            self._reader_loop(), name="mcp-reader:%s" % self.name
        )
        self._stderr_task = asyncio.create_task(
            self._stderr_loop(), name="mcp-stderr:%s" % self.name
        )
        await self._request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "kirakira-agent", "version": "0.1.0"},
            },
            timeout=self.connect_timeout,
        )
        await self._notify("notifications/initialized", {})
        response = await self._request("tools/list", {}, timeout=self.connect_timeout)
        raw_tools = response.get("result", {}).get("tools", [])
        if not isinstance(raw_tools, list):
            raise RuntimeError("MCP tools/list returned a non-list payload")
        self.tool_infos = [self._parse_tool(item) for item in raw_tools]

    async def call(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
        *,
        timeout: Optional[float] = None,
    ) -> str:
        response = await self._request(
            "tools/call",
            {"name": tool_name, "arguments": dict(arguments)},
            timeout=timeout,
        )
        if "error" in response:
            error = response.get("error")
            # JSON-RPC error 必须是对象；不是就原样上报，不假装它是一条 message。
            message = error.get("message", error) if isinstance(error, dict) else error
            return "Error: MCP %s/%s: %s" % (self.name, tool_name, message)
        # 协议要求 result 是对象且 content 是数组。不符合就直接判错，
        # 而不是把畸形结构硬塞给 _content_text 拼出一段看起来正常的文本。
        result = response.get("result")
        if not isinstance(result, dict):
            return "Error: MCP %s/%s returned a non-object result" % (
                self.name,
                tool_name,
            )
        content = result.get("content", [])
        if not isinstance(content, list):
            return "Error: MCP %s/%s returned a non-list content field" % (
                self.name,
                tool_name,
            )
        text = self._content_text(content)
        if result.get("isError"):
            return "Error: MCP %s/%s: %s" % (self.name, tool_name, text)
        return text

    async def disconnect(self) -> None:
        process = self._process
        self._process = None
        self._fail_pending(ConnectionError("MCP server disconnected: %s" % self.name))
        for task in (self._reader_task, self._stderr_task):
            if task is not None and not task.done():
                task.cancel()
        tasks = [task for task in (self._reader_task, self._stderr_task) if task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._reader_task = None
        self._stderr_task = None
        if process is None or process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()

    async def _request(
        self,
        method: str,
        params: Dict[str, Any],
        *,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        process = self._process
        if process is None or process.returncode is not None:
            raise ConnectionError("MCP server is not connected: %s" % self.name)
        request_id = self._next_id
        self._next_id += 1
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            )
            wait_timeout = self.request_timeout if timeout is None else max(0.1, timeout)
            return await asyncio.wait_for(future, timeout=wait_timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(self._timeout_message(method, request_id)) from exc
        finally:
            self._pending.pop(request_id, None)

    async def _notify(self, method: str, params: Dict[str, Any]) -> None:
        await self._send({"jsonrpc": "2.0", "method": method, "params": params})

    async def _send(self, payload: Dict[str, Any]) -> None:
        process = self._process
        if process is None or process.stdin is None:
            raise ConnectionError("MCP stdin is unavailable: %s" % self.name)
        encoded = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        async with self._send_lock:
            process.stdin.write(encoded)
            await process.stdin.drain()

    async def _reader_loop(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    raise ConnectionError("MCP server closed stdout: %s" % self.name)
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                self._recent_stdout.append(text[:500])
                try:
                    message = json.loads(text)
                except ValueError:
                    logger.debug("MCP %s emitted non-JSON stdout: %s", self.name, text[:200])
                    continue
                request_id = message.get("id") if isinstance(message, dict) else None
                future = self._pending.get(request_id)
                if future is not None and not future.done():
                    future.set_result(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_pending(exc)

    async def _stderr_loop(self) -> None:
        process = self._process
        if process is None or process.stderr is None:
            return
        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    return
                text = line.decode("utf-8", errors="replace").rstrip()
                self._recent_stderr.append(text[:500])
                logger.debug("MCP %s stderr: %s", self.name, text)
        except asyncio.CancelledError:
            raise

    def _fail_pending(self, exc: BaseException) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(exc)

    def _timeout_message(self, method: str, request_id: int) -> str:
        details = [
            "MCP server %r timed out during %s (id=%s)" % (self.name, method, request_id),
            "command=%r" % self.command,
        ]
        if self.cwd:
            details.append("cwd=%s" % self.cwd)
        if self._recent_stdout:
            details.append("recent_stdout=" + " | ".join(self._recent_stdout))
        if self._recent_stderr:
            details.append("recent_stderr=" + " | ".join(self._recent_stderr))
        return "; ".join(details)

    @staticmethod
    def _parse_tool(item: object) -> McpToolInfo:
        if not isinstance(item, dict) or not str(item.get("name") or "").strip():
            raise RuntimeError("MCP tools/list contains an invalid tool")
        schema = item.get("inputSchema")
        if not isinstance(schema, dict):
            schema = {"type": "object", "properties": {}}
        return McpToolInfo(
            name=str(item["name"]),
            description=str(item.get("description") or ""),
            input_schema=schema,
        )

    @staticmethod
    def _content_text(content: object) -> str:
        if not isinstance(content, list):
            return str(content)
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text") or ""))
            else:
                parts.append(json.dumps(block, ensure_ascii=False, default=str))
        return "\n".join(part for part in parts if part)

    @staticmethod
    def _infer_cwd(command: List[str]) -> Optional[str]:
        for argument in command:
            path = Path(argument).expanduser()
            if path.is_absolute() and path.is_file():
                return str(path.parent)
        return None
