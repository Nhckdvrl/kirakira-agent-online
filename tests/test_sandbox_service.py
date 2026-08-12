from __future__ import annotations

from pathlib import Path
import shutil

from fastapi.testclient import TestClient
import pytest

from sandbox_service.api import create_app


TOKEN = "sandbox-test-token-at-least-24-chars"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


@pytest.fixture()
def client(tmp_path: Path):
    bwrap = shutil.which("bwrap")
    if not bwrap:
        pytest.skip("bwrap is not installed")
    app = create_app(root=tmp_path, auth_token=TOKEN, bwrap_path=Path(bwrap))
    with TestClient(app) as value:
        yield value


def test_capabilities_require_auth_and_truthfully_attest(client: TestClient) -> None:
    assert client.get("/v1/capabilities").status_code == 401
    response = client.get("/v1/capabilities", headers=AUTH)
    assert response.status_code == 200
    assert response.json() == {
        "name": "bubblewrap-namespace",
        "isolated": True,
        "host_execution": False,
        "workspace_isolated": True,
        "network_default": "deny",
    }


def test_owner_workspaces_are_isolated_and_traversal_is_rejected(client: TestClient) -> None:
    first = {"owner": "alice:conversation", "path": "notes/a.txt", "content": "alice"}
    assert client.post("/v1/workspace/write", headers=AUTH, json=first).status_code == 200
    read = client.post(
        "/v1/workspace/read",
        headers=AUTH,
        json={"owner": "alice:conversation", "path": "notes/a.txt", "offset": 0},
    )
    assert read.json()["result"] == "alice"
    missing = client.post(
        "/v1/workspace/read",
        headers=AUTH,
        json={"owner": "bob:conversation", "path": "notes/a.txt", "offset": 0},
    )
    assert missing.status_code == 404
    escaped = client.post(
        "/v1/workspace/write",
        headers=AUTH,
        json={"owner": "alice:conversation", "path": "../escape", "content": "bad"},
    )
    assert escaped.status_code == 422
    binary = client.post(
        "/v1/workspace/write-binary",
        headers=AUTH,
        json={
            "owner": "alice:conversation",
            "path": "images/raw.bin",
            "content_base64": "AAEC/w==",
        },
    )
    assert binary.status_code == 200
    fetched = client.post(
        "/v1/workspace/read-binary",
        headers=AUTH,
        json={"owner": "alice:conversation", "path": "images/raw.bin"},
    )
    assert fetched.json()["content_base64"] == "AAEC/w=="


def test_command_runs_in_owner_workspace_without_host_network(client: TestClient) -> None:
    response = client.post(
        "/v1/executions",
        headers=AUTH,
        json={
            "command": "pwd; printf hello > result.txt",
            "argv": ["/bin/sh", "-c", "pwd; printf hello > result.txt"],
            "owner": "alice:conversation",
            "yield_time_ms": 5_000,
            "max_output_tokens": 1_000,
            "hard_timeout_seconds": 10,
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["exit_code"] == 0
    assert "/workspace" in response.json()["output"]
    read = client.post(
        "/v1/workspace/read",
        headers=AUTH,
        json={"owner": "alice:conversation", "path": "result.txt", "offset": 0},
    )
    assert read.json()["result"] == "hello"


def test_execution_owner_cannot_control_another_owners_process(client: TestClient) -> None:
    started = client.post(
        "/v1/executions",
        headers=AUTH,
        json={
            "command": "sleep 30",
            "argv": ["/bin/sh", "-c", "sleep 30"],
            "owner": "alice:conversation",
            "return_immediately": True,
            "yield_time_ms": 0,
            "max_output_tokens": 1_000,
            "hard_timeout_seconds": 60,
        },
    ).json()
    execution_id = started["execution_id"]
    denied = client.post(
        f"/v1/executions/{execution_id}/terminate",
        headers=AUTH,
        json={"owner": "bob:conversation"},
    )
    assert denied.json()["terminated"] is False
    stopped = client.post(
        f"/v1/executions/{execution_id}/terminate",
        headers=AUTH,
        json={"owner": "alice:conversation"},
    )
    assert stopped.json()["terminated"] is True
