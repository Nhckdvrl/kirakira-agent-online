"""Authenticated execution and workspace API backed by Bubblewrap namespaces."""

from __future__ import annotations

import base64
from contextlib import asynccontextmanager
from hashlib import sha256
import hmac
import os
from pathlib import Path
import shutil
from typing import Annotated, AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

from agent.tools.unified_exec import ExecutionResult, ShellProcessManager


class ExecutionIn(BaseModel):
    command: str = ""
    argv: list[str] = Field(min_length=1, max_length=64)
    environment: dict[str, str] = Field(default_factory=dict)
    tty: bool = False
    yield_time_ms: int = Field(default=10_000, ge=0, le=30_000)
    max_output_tokens: int = Field(default=10_000, ge=1, le=100_000)
    hard_timeout_seconds: int = Field(default=120, ge=1, le=3_600)
    owner: str = Field(min_length=1, max_length=512)
    return_immediately: bool = False


class InputIn(BaseModel):
    chars: str = Field(default="", max_length=1_000_000)
    yield_time_ms: int = Field(default=10_000, ge=0, le=300_000)
    max_output_tokens: int = Field(default=10_000, ge=1, le=100_000)
    owner: str = Field(min_length=1, max_length=512)
    return_immediately: bool = False


class OwnerIn(BaseModel):
    owner: str = Field(min_length=1, max_length=512)


class WorkspaceReadIn(OwnerIn):
    path: str = Field(min_length=1, max_length=4_096)
    limit: int | None = Field(default=None, ge=0, le=4_000_000)
    offset: int = Field(default=0, ge=0, le=4_000_000)


class WorkspacePathIn(OwnerIn):
    path: str = Field(min_length=1, max_length=4_096)


class WorkspaceWriteIn(WorkspacePathIn):
    content: str = Field(max_length=4_000_000)


class WorkspaceBinaryWriteIn(WorkspacePathIn):
    content_base64: str = Field(max_length=24_000_000)


class WorkspaceEditIn(WorkspacePathIn):
    old_text: str = Field(max_length=4_000_000)
    new_text: str = Field(max_length=4_000_000)
    replace_all: bool = False


class SandboxRuntime:
    """Own process state and tenant workspaces for one sandbox service process."""

    def __init__(self, root: Path, bwrap_path: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.bwrap_path = bwrap_path.resolve()
        self.executions = ShellProcessManager(
            max_executions=max(1, int(os.getenv("KIRAKIRA_SANDBOX_MAX_EXECUTIONS", "64")))
        )

    def workspace(self, owner: str) -> Path:
        key = sha256(owner.encode("utf-8")).hexdigest()
        path = self.root / key[:2] / key
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        return path

    def safe_path(self, owner: str, raw: str) -> Path:
        workspace = self.workspace(owner)
        relative = Path(raw)
        if relative.is_absolute():
            raise ValueError("workspace path must be relative")
        candidate = workspace / relative
        current = workspace
        for part in relative.parts:
            if part in {"", "."}:
                continue
            if part == "..":
                raise ValueError("workspace path escapes owner root")
            current = current / part
            if current.is_symlink():
                raise ValueError("workspace symlinks are not allowed through the API")
        resolved_parent = candidate.parent.resolve()
        if resolved_parent != workspace and workspace not in resolved_parent.parents:
            raise ValueError("workspace path escapes owner root")
        return candidate

    def bwrap_argv(self, owner: str, argv: list[str]) -> list[str]:
        executable = argv[0]
        if not executable.startswith("/") or not any(
            executable == prefix or executable.startswith(prefix + "/")
            for prefix in ("/bin", "/usr/bin", "/usr/local/bin")
        ):
            raise ValueError("sandbox executable must use an allowed absolute path")
        args = [
            str(self.bwrap_path),
            "--unshare-all",
            "--die-with-parent",
            "--new-session",
            "--clearenv",
            "--setenv", "PATH", "/usr/local/bin:/usr/bin:/bin",
            "--setenv", "HOME", "/workspace",
            "--setenv", "TMPDIR", "/tmp",
            "--ro-bind", "/usr", "/usr",
            "--ro-bind", "/bin", "/bin",
            "--proc", "/proc",
            "--dev", "/dev",
            "--tmpfs", "/tmp",
        ]
        for source in ("/lib", "/lib64"):
            if Path(source).exists():
                args.extend(("--ro-bind", source, source))
        args.extend(("--bind", str(self.workspace(owner)), "/workspace"))
        args.extend(("--chdir", "/workspace", "--"))
        args.extend(argv)
        return args


def create_app(
    *,
    root: Path | None = None,
    auth_token: str | None = None,
    bwrap_path: Path | None = None,
) -> FastAPI:
    token = auth_token if auth_token is not None else os.getenv("KIRAKIRA_SANDBOX_TOKEN", "")
    if len(token) < 24:
        raise RuntimeError("KIRAKIRA_SANDBOX_TOKEN must contain at least 24 characters")
    resolved_bwrap = bwrap_path or Path(shutil.which("bwrap") or "")
    if not resolved_bwrap.is_file():
        raise RuntimeError("Bubblewrap (bwrap) is required")
    runtime = SandboxRuntime(
        root or Path(os.getenv("KIRAKIRA_SANDBOX_ROOT", "/var/lib/kirakira-sandbox")),
        resolved_bwrap,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        await runtime.executions.shutdown()

    app = FastAPI(title="Kirakira Isolated Sandbox", version="0.1.0", lifespan=lifespan)
    app.state.runtime = runtime

    def authenticate(authorization: Annotated[str | None, Header()] = None) -> None:
        supplied = authorization or ""
        expected = f"Bearer {token}"
        if not hmac.compare_digest(supplied, expected):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid sandbox credential")

    protected = [Depends(authenticate)]

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/capabilities", dependencies=protected)
    async def capabilities() -> dict[str, object]:
        return {
            "name": "bubblewrap-namespace",
            "isolated": True,
            "host_execution": False,
            "workspace_isolated": True,
            "network_default": "deny",
        }

    @app.post("/v1/executions", dependencies=protected)
    async def execute(payload: ExecutionIn) -> dict[str, object]:
        try:
            argv = runtime.bwrap_argv(payload.owner, payload.argv)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        allowed_env = {
            key: value
            for key, value in payload.environment.items()
            if key in {"LANG", "LC_ALL", "TERM", "TZ"} and len(value) <= 1_024
        }
        result = await runtime.executions.exec_command(
            command=payload.command,
            argv=argv,
            cwd=None,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), **allowed_env},
            tty=payload.tty,
            yield_time_ms=payload.yield_time_ms,
            max_output_tokens=payload.max_output_tokens,
            hard_timeout_s=payload.hard_timeout_seconds,
            owner_session_key=payload.owner,
            return_immediately=payload.return_immediately,
        )
        return _result(result)

    @app.post("/v1/executions/{execution_id}/input", dependencies=protected)
    async def write_input(execution_id: int, payload: InputIn) -> dict[str, object]:
        try:
            result = await runtime.executions.write_stdin(
                execution_id=execution_id,
                chars=payload.chars,
                yield_time_ms=payload.yield_time_ms,
                max_output_tokens=payload.max_output_tokens,
                owner_session_key=payload.owner,
                return_immediately=payload.return_immediately,
            )
        except (KeyError, RuntimeError) as exc:
            raise HTTPException(404, str(exc)) from exc
        return _result(result)

    @app.post("/v1/executions/{execution_id}/terminate", dependencies=protected)
    async def terminate(execution_id: int, payload: OwnerIn) -> dict[str, bool]:
        return {
            "terminated": await runtime.executions.terminate_execution(
                execution_id, owner_session_key=payload.owner
            )
        }

    @app.post("/v1/executions/terminate-owner", dependencies=protected)
    async def terminate_owner(payload: OwnerIn) -> dict[str, object]:
        report = await runtime.executions.terminate_owner(payload.owner)
        return {
            "attempted_execution_ids": list(report.attempted_execution_ids),
            "cleaned_execution_ids": list(report.cleaned_execution_ids),
            "failures": [
                {
                    "execution_id": item.execution_id,
                    "error_type": item.error_type,
                    "message": item.message,
                }
                for item in report.failures
            ],
        }

    @app.post("/v1/workspace/read", dependencies=protected)
    async def read_file(payload: WorkspaceReadIn) -> dict[str, str]:
        path = _safe(runtime, payload.owner, payload.path)
        if not path.is_file():
            raise HTTPException(404, "file not found")
        data = path.read_bytes()
        start = min(payload.offset, len(data))
        end = len(data) if payload.limit is None else min(len(data), start + payload.limit)
        return {"result": data[start:end].decode("utf-8", errors="replace")}

    @app.post("/v1/workspace/read-binary", dependencies=protected)
    async def read_binary(payload: WorkspacePathIn) -> dict[str, str]:
        path = _safe(runtime, payload.owner, payload.path)
        if not path.is_file():
            raise HTTPException(404, "file not found")
        data = path.read_bytes()
        if len(data) > 16_000_000:
            raise HTTPException(413, "file exceeds binary read limit")
        return {"content_base64": base64.b64encode(data).decode("ascii")}

    @app.post("/v1/workspace/list", dependencies=protected)
    async def list_dir(payload: WorkspacePathIn) -> dict[str, str]:
        path = _safe(runtime, payload.owner, payload.path)
        if not path.is_dir():
            raise HTTPException(404, "directory not found")
        lines = []
        for child in sorted(path.iterdir(), key=lambda item: item.name):
            kind = "dir" if child.is_dir() else "file"
            lines.append(f"{kind}\t{child.name}")
        return {"result": "\n".join(lines)}

    @app.post("/v1/workspace/write", dependencies=protected)
    async def write_file(payload: WorkspaceWriteIn) -> dict[str, str]:
        path = _safe(runtime, payload.owner, payload.path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.write_text(payload.content, encoding="utf-8")
        return {"result": f"Wrote {payload.path}"}

    @app.post("/v1/workspace/write-binary", dependencies=protected)
    async def write_binary(payload: WorkspaceBinaryWriteIn) -> dict[str, str]:
        path = _safe(runtime, payload.owner, payload.path)
        try:
            content = base64.b64decode(payload.content_base64, validate=True)
        except ValueError as exc:
            raise HTTPException(422, "invalid base64 content") from exc
        if len(content) > 16_000_000:
            raise HTTPException(413, "file exceeds upload limit")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.write_bytes(content)
        return {"result": f"Wrote {payload.path}"}

    @app.post("/v1/workspace/edit", dependencies=protected)
    async def edit_file(payload: WorkspaceEditIn) -> dict[str, str]:
        path = _safe(runtime, payload.owner, payload.path)
        if not path.is_file():
            raise HTTPException(404, "file not found")
        content = path.read_text(encoding="utf-8")
        count = content.count(payload.old_text)
        if count == 0:
            raise HTTPException(409, "old_text not found")
        if count > 1 and not payload.replace_all:
            raise HTTPException(409, "old_text is not unique")
        updated = content.replace(
            payload.old_text, payload.new_text, -1 if payload.replace_all else 1
        )
        path.write_text(updated, encoding="utf-8")
        return {"result": f"Edited {payload.path}"}

    return app


def _safe(runtime: SandboxRuntime, owner: str, raw: str) -> Path:
    try:
        return runtime.safe_path(owner, raw)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


def _result(result: ExecutionResult) -> dict[str, object]:
    return {
        "output": result.output.decode("utf-8", errors="replace"),
        "wall_time_ms": result.wall_time_ms,
        "original_token_count": result.original_token_count,
        "output_omitted_bytes": result.output_omitted_bytes,
        "execution_id": result.execution_id,
        "exit_code": result.exit_code,
        "finish_reason": result.finish_reason,
    }
