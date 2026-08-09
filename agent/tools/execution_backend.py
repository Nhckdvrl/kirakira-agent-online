"""Deployment boundary for shell execution used by Agent tools."""

from __future__ import annotations

from dataclasses import dataclass
import base64
from pathlib import Path
from typing import Protocol

import httpx

from agent.tools.unified_exec import (
    ExecutionCleanupFailure,
    ExecutionCleanupReport,
    ExecutionResult,
    ShellProcessManager,
)


@dataclass(frozen=True)
class ExecutionBackendDescriptor:
    name: str
    isolated: bool
    host_execution: bool
    workspace_isolated: bool


class ExecutionBackend(Protocol):
    descriptor: ExecutionBackendDescriptor

    async def probe(self) -> ExecutionBackendDescriptor: ...

    async def exec_command(
        self,
        *,
        command: str,
        argv: list[str],
        cwd: Path | None,
        env: dict[str, str],
        tty: bool,
        yield_time_ms: int,
        max_output_tokens: int,
        hard_timeout_s: int,
        owner_session_key: str,
        return_immediately: bool = False,
    ) -> ExecutionResult: ...

    async def write_stdin(
        self,
        *,
        execution_id: int,
        chars: str,
        yield_time_ms: int,
        max_output_tokens: int,
        owner_session_key: str,
        return_immediately: bool = False,
    ) -> ExecutionResult: ...

    async def terminate_execution(
        self, execution_id: int, *, owner_session_key: str
    ) -> bool: ...

    async def terminate_owner(self, owner_session_key: str) -> ExecutionCleanupReport: ...
    async def shutdown(self) -> ExecutionCleanupReport: ...


class WorkspaceBackend(Protocol):
    async def read_file(self, owner: str, path: str, limit: int | None, offset: int) -> str: ...
    async def list_dir(self, owner: str, path: str) -> str: ...
    async def write_file(self, owner: str, path: str, content: str) -> str: ...
    async def edit_file(
        self, owner: str, path: str, old_text: str, new_text: str, replace_all: bool
    ) -> str: ...
    async def read_binary(self, owner: str, path: str) -> bytes: ...


class LocalExecutionBackend:
    """Compatibility backend for the local adapter only."""

    descriptor = ExecutionBackendDescriptor(
        name="local-process",
        isolated=False,
        host_execution=True,
        workspace_isolated=False,
    )

    def __init__(self, manager: ShellProcessManager | None = None) -> None:
        self._manager = manager or ShellProcessManager()

    async def probe(self) -> ExecutionBackendDescriptor:
        return self.descriptor

    async def exec_command(self, **kwargs) -> ExecutionResult:
        return await self._manager.exec_command(**kwargs)

    async def write_stdin(self, **kwargs) -> ExecutionResult:
        return await self._manager.write_stdin(**kwargs)

    async def terminate_execution(
        self, execution_id: int, *, owner_session_key: str
    ) -> bool:
        return await self._manager.terminate_execution(
            execution_id, owner_session_key=owner_session_key
        )

    async def terminate_owner(self, owner_session_key: str) -> ExecutionCleanupReport:
        return await self._manager.terminate_owner(owner_session_key)

    async def shutdown(self) -> ExecutionCleanupReport:
        return await self._manager.shutdown()


class RemoteSandboxExecutionBackend:
    """Client for an external sandbox service; never spawns on the Agent host."""

    def __init__(
        self,
        base_url: str,
        *,
        auth_token: str = "",
        timeout_seconds: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        url = base_url.rstrip("/")
        if not url:
            raise ValueError("remote sandbox base_url is required")
        self.descriptor = ExecutionBackendDescriptor(
            name="remote-sandbox-unverified",
            isolated=False,
            host_execution=True,
            workspace_isolated=False,
        )
        headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
        self._client = client or httpx.AsyncClient(
            base_url=url,
            headers=headers,
            timeout=max(1.0, timeout_seconds),
        )
        self._owns_client = client is None

    async def probe(self) -> ExecutionBackendDescriptor:
        response = await self._client.get("/v1/capabilities")
        response.raise_for_status()
        data = response.json()
        descriptor = ExecutionBackendDescriptor(
            name=str(data.get("name") or "remote-sandbox"),
            isolated=data.get("isolated") is True,
            host_execution=data.get("host_execution") is True,
            workspace_isolated=data.get("workspace_isolated") is True,
        )
        if (
            not descriptor.isolated
            or descriptor.host_execution
            or not descriptor.workspace_isolated
        ):
            raise RuntimeError(
                "sandbox capability attestation does not satisfy Cloud isolation"
            )
        self.descriptor = descriptor
        return descriptor

    async def exec_command(
        self,
        *,
        command: str,
        argv: list[str],
        cwd: Path | None,
        env: dict[str, str],
        tty: bool,
        yield_time_ms: int,
        max_output_tokens: int,
        hard_timeout_s: int,
        owner_session_key: str,
        return_immediately: bool = False,
    ) -> ExecutionResult:
        del cwd  # Host paths are never sent to or mounted in the sandbox.
        return await self._result_request(
            "/v1/executions",
            {
                "command": command,
                "argv": argv,
                "environment": env,
                "tty": tty,
                "yield_time_ms": yield_time_ms,
                "max_output_tokens": max_output_tokens,
                "hard_timeout_seconds": hard_timeout_s,
                "owner": owner_session_key,
                "return_immediately": return_immediately,
            },
        )

    async def write_stdin(
        self,
        *,
        execution_id: int,
        chars: str,
        yield_time_ms: int,
        max_output_tokens: int,
        owner_session_key: str,
        return_immediately: bool = False,
    ) -> ExecutionResult:
        return await self._result_request(
            f"/v1/executions/{execution_id}/input",
            {
                "chars": chars,
                "yield_time_ms": yield_time_ms,
                "max_output_tokens": max_output_tokens,
                "owner": owner_session_key,
                "return_immediately": return_immediately,
            },
        )

    async def terminate_execution(
        self, execution_id: int, *, owner_session_key: str
    ) -> bool:
        response = await self._client.post(
            f"/v1/executions/{execution_id}/terminate",
            json={"owner": owner_session_key},
        )
        response.raise_for_status()
        return bool(response.json().get("terminated", False))

    async def terminate_owner(self, owner_session_key: str) -> ExecutionCleanupReport:
        response = await self._client.post(
            "/v1/executions/terminate-owner", json={"owner": owner_session_key}
        )
        response.raise_for_status()
        return self._cleanup_report(response.json())

    async def shutdown(self) -> ExecutionCleanupReport:
        if self._owns_client:
            await self._client.aclose()
        return ExecutionCleanupReport((), (), ())

    async def read_file(
        self, owner: str, path: str, limit: int | None, offset: int
    ) -> str:
        return await self._workspace_result(
            "/v1/workspace/read",
            {"owner": owner, "path": path, "limit": limit, "offset": offset},
        )

    async def list_dir(self, owner: str, path: str) -> str:
        return await self._workspace_result(
            "/v1/workspace/list", {"owner": owner, "path": path}
        )

    async def write_file(self, owner: str, path: str, content: str) -> str:
        return await self._workspace_result(
            "/v1/workspace/write",
            {"owner": owner, "path": path, "content": content},
        )

    async def edit_file(
        self,
        owner: str,
        path: str,
        old_text: str,
        new_text: str,
        replace_all: bool,
    ) -> str:
        return await self._workspace_result(
            "/v1/workspace/edit",
            {
                "owner": owner,
                "path": path,
                "old_text": old_text,
                "new_text": new_text,
                "replace_all": replace_all,
            },
        )

    async def read_binary(self, owner: str, path: str) -> bytes:
        response = await self._client.post(
            "/v1/workspace/read-binary", json={"owner": owner, "path": path}
        )
        response.raise_for_status()
        return base64.b64decode(str(response.json().get("content_base64") or ""))

    async def _result_request(self, path: str, payload: dict[str, object]) -> ExecutionResult:
        response = await self._client.post(path, json=payload)
        response.raise_for_status()
        data = response.json()
        return ExecutionResult(
            output=str(data.get("output") or "").encode(),
            wall_time_ms=int(data.get("wall_time_ms") or 0),
            original_token_count=int(data.get("original_token_count") or 0),
            output_omitted_bytes=int(data.get("output_omitted_bytes") or 0),
            execution_id=(
                int(data["execution_id"])
                if data.get("execution_id") is not None
                else None
            ),
            exit_code=(
                int(data["exit_code"]) if data.get("exit_code") is not None else None
            ),
            output_path=None,
            finish_reason=str(data.get("finish_reason") or "natural"),
        )

    async def _workspace_result(self, path: str, payload: dict[str, object]) -> str:
        response = await self._client.post(path, json=payload)
        response.raise_for_status()
        return str(response.json().get("result") or "")

    @staticmethod
    def _cleanup_report(data: dict[str, object]) -> ExecutionCleanupReport:
        raw_failures = data.get("failures", []) or []
        failures = tuple(
            ExecutionCleanupFailure(
                execution_id=int(item.get("execution_id") or 0),
                error_type=str(item.get("error_type") or "SandboxCleanupError"),
                message=str(item.get("message") or "sandbox cleanup failed"),
            )
            for item in raw_failures
            if isinstance(item, dict)
        )
        return ExecutionCleanupReport(
            tuple(int(value) for value in data.get("attempted_execution_ids", []) or []),
            tuple(int(value) for value in data.get("cleaned_execution_ids", []) or []),
            failures,
        )
