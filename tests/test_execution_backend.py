from __future__ import annotations

import json

import httpx
import pytest

from agent.tools.execution_backend import RemoteSandboxExecutionBackend


def _response(payload: dict, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload)


@pytest.mark.asyncio
async def test_remote_sandbox_requires_verified_isolation_and_preserves_owner() -> None:
    seen: list[tuple[str, dict]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content or b"{}")
        seen.append((request.url.path, payload))
        if request.url.path == "/v1/capabilities":
            return _response(
                {
                    "name": "gvisor-pool",
                    "isolated": True,
                    "host_execution": False,
                    "workspace_isolated": True,
                }
            )
        if request.url.path == "/v1/executions":
            return _response(
                {
                    "output": "started",
                    "wall_time_ms": 12,
                    "original_token_count": 1,
                    "output_omitted_bytes": 0,
                    "execution_id": 41,
                    "exit_code": None,
                    "finish_reason": "yield",
                }
            )
        if request.url.path == "/v1/executions/41/input":
            return _response(
                {
                    "output": "done",
                    "wall_time_ms": 2,
                    "original_token_count": 1,
                    "output_omitted_bytes": 0,
                    "execution_id": None,
                    "exit_code": 0,
                    "finish_reason": "natural",
                }
            )
        if request.url.path == "/v1/executions/terminate-owner":
            return _response(
                {
                    "attempted_execution_ids": [41, 42],
                    "cleaned_execution_ids": [41],
                    "failures": [
                        {
                            "execution_id": 42,
                            "error_type": "TimeoutError",
                            "message": "kill not confirmed",
                        }
                    ],
                }
            )
        raise AssertionError(request.url.path)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://sandbox.test"
    )
    backend = RemoteSandboxExecutionBackend(
        "https://sandbox.test", auth_token="secret", client=client
    )
    descriptor = await backend.probe()
    assert descriptor.name == "gvisor-pool" and descriptor.isolated
    started = await backend.exec_command(
        command="pwd",
        argv=["/bin/sh", "-c", "pwd"],
        cwd=None,
        env={"LANG": "C.UTF-8"},
        tty=True,
        yield_time_ms=1000,
        max_output_tokens=100,
        hard_timeout_s=30,
        owner_session_key="conversation-a",
    )
    assert started.execution_id == 41
    finished = await backend.write_stdin(
        execution_id=41,
        chars="",
        yield_time_ms=1000,
        max_output_tokens=100,
        owner_session_key="conversation-a",
    )
    assert finished.exit_code == 0
    report = await backend.terminate_owner("conversation-a")
    assert report.cleaned_execution_ids == (41,)
    assert report.failed_execution_ids == (42,)
    assert all(
        payload.get("owner") == "conversation-a"
        for path, payload in seen
        if path != "/v1/capabilities"
    )
    await client.aclose()


@pytest.mark.asyncio
async def test_remote_sandbox_rejects_unisolated_capability_claim() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return _response(
            {
                "name": "host-shell",
                "isolated": False,
                "host_execution": True,
                "workspace_isolated": False,
            }
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://sandbox.test"
    )
    backend = RemoteSandboxExecutionBackend("https://sandbox.test", client=client)
    with pytest.raises(RuntimeError, match="attestation"):
        await backend.probe()
    await client.aclose()
