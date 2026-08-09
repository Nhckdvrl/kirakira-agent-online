"""控制面:状态机、turn 编排、JSON-RPC 协议与接线。"""

from __future__ import annotations

import asyncio
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from agent.control.errors import (
    ThreadBusyError,
    TurnNotFoundError,
    TurnStateTransitionError,
)
from agent.control.ids import new_item_id, new_turn_id
from agent.control.models import (
    TurnItem,
    TurnItemKind,
    TurnRecord,
    TurnRequest,
    TurnStatus,
)
from agent.control.ports import ControlExecutionResult
from agent.control.protocol.models import METHOD_PARAMS, ParamValidationError
from agent.control.runtime import ConversationRuntime
from agent.control.service import ControlService
from infra.control.socket import SocketAppServer
from agent.control.store import ControlStore
from session.manager import SessionManager


@pytest.fixture
def workspace():
    with tempfile.TemporaryDirectory() as raw:
        yield Path(raw)


@pytest.fixture
def store(workspace):
    store = ControlStore(workspace / "control.db")
    yield store
    store.close()


def _queued(thread_id: str = "t1") -> TurnRecord:
    return TurnRecord(
        id=new_turn_id(),
        thread_id=thread_id,
        status=TurnStatus.QUEUED,
        input="hello",
        items=[TurnItem(TurnItemKind.USER_MESSAGE, new_item_id(), {"content": "hello"})],
        created_at=datetime.now(UTC),
    )


# --- store 状态机 ---------------------------------------------------------


def test_create_turn_rejects_non_queued(store):
    record = _queued()
    bad = TurnRecord(
        id=record.id,
        thread_id=record.thread_id,
        status=TurnStatus.IN_PROGRESS,
        input=record.input,
        created_at=record.created_at,
    )
    with pytest.raises(TurnStateTransitionError):
        store.create_turn(bad)


def test_transition_sets_timestamps_and_is_cas_guarded(store):
    record = store.create_turn(_queued())
    running = store.transition_turn(
        record.id,
        expected_status=TurnStatus.QUEUED,
        status=TurnStatus.IN_PROGRESS,
        thread_id="t1",
    )
    assert running.started_at is not None and running.completed_at is None
    done = store.transition_turn(
        record.id,
        expected_status=TurnStatus.IN_PROGRESS,
        status=TurnStatus.COMPLETED,
        thread_id="t1",
        final_response="hi",
    )
    assert done.completed_at is not None and done.duration_ms is not None
    # 同一次 CAS 不能重放
    with pytest.raises(TurnStateTransitionError):
        store.transition_turn(
            record.id,
            expected_status=TurnStatus.IN_PROGRESS,
            status=TurnStatus.COMPLETED,
            thread_id="t1",
        )


def test_illegal_transition_and_wrong_thread_are_rejected(store):
    record = store.create_turn(_queued())
    with pytest.raises(TurnStateTransitionError):
        # queued 不能直接跳终态 completed
        store.transition_turn(
            record.id, expected_status=TurnStatus.QUEUED, status=TurnStatus.COMPLETED
        )
    with pytest.raises(TurnNotFoundError):
        store.transition_turn(
            record.id,
            expected_status=TurnStatus.QUEUED,
            status=TurnStatus.IN_PROGRESS,
            thread_id="other-thread",
        )


def test_failed_turn_requires_error(store):
    record = store.create_turn(_queued())
    store.transition_turn(
        record.id, expected_status=TurnStatus.QUEUED, status=TurnStatus.IN_PROGRESS
    )
    with pytest.raises(TurnStateTransitionError):
        store.transition_turn(
            record.id,
            expected_status=TurnStatus.IN_PROGRESS,
            status=TurnStatus.FAILED,
        )


# --- ConversationRuntime --------------------------------------------------


@pytest.mark.asyncio
async def test_turn_emits_ordered_lifecycle_events(store):
    async def executor(request: TurnRequest) -> ControlExecutionResult:
        return ControlExecutionResult(response="pong", deltas=["po", "ng"])

    runtime = ConversationRuntime(store, executor)
    handle = await runtime.start_turn(TurnRequest("t1", "ping"))
    methods = [event.method async for event in handle.events()]
    assert methods[0] == "turn/queued"
    assert "turn/started" in methods
    assert methods.count("item/assistantMessage/delta") == 2
    assert methods[-1] == "turn/completed"
    result = await handle.result()
    assert result.status is TurnStatus.COMPLETED
    assert result.final_response == "pong"
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_same_thread_rejects_concurrent_turn(store):
    release = asyncio.Event()

    async def executor(request: TurnRequest) -> ControlExecutionResult:
        await release.wait()
        return ControlExecutionResult(response="ok")

    runtime = ConversationRuntime(store, executor)
    first = await runtime.start_turn(TurnRequest("t1", "a"))
    await asyncio.sleep(0)
    with pytest.raises(ThreadBusyError):
        await runtime.start_turn(TurnRequest("t1", "b"))
    # 不同 thread 不受影响
    release.set()
    await first.result()
    second = await runtime.start_turn(TurnRequest("t2", "c"))
    assert (await second.result()).status is TurnStatus.COMPLETED
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_failed_executor_writes_failed_terminal_with_error(store):
    async def executor(request: TurnRequest) -> ControlExecutionResult:
        raise ValueError("boom")

    runtime = ConversationRuntime(store, executor)
    handle = await runtime.start_turn(TurnRequest("t1", "a"))
    result = await handle.result()
    assert result.status is TurnStatus.FAILED
    assert result.error is not None
    assert result.error.type == "ValueError" and "boom" in result.error.message
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_interrupt_marks_turn_interrupted_and_frees_thread(store):
    async def executor(request: TurnRequest) -> ControlExecutionResult:
        await asyncio.sleep(30)
        return ControlExecutionResult(response="never")

    runtime = ConversationRuntime(store, executor)
    handle = await runtime.start_turn(TurnRequest("t1", "slow"))
    await asyncio.sleep(0.05)
    record = await runtime.interrupt_turn("t1", handle.id)
    assert record.status is TurnStatus.INTERRUPTED
    assert not runtime.is_thread_active("t1")
    await runtime.shutdown()


# --- 协议参数校验 ----------------------------------------------------------


def test_params_reject_unknown_fields():
    with pytest.raises(ParamValidationError) as exc:
        METHOD_PARAMS["thread/resume"].validate({"threadId": "t1", "bogus": 1})
    assert [issue["type"] for issue in exc.value.issues] == ["extra_forbidden"]


def test_params_are_strict_about_types():
    # bool 不能当 int 用
    with pytest.raises(ParamValidationError):
        METHOD_PARAMS["thread/list"].validate({"limit": True})
    # int 不能当 str 用
    with pytest.raises(ParamValidationError):
        METHOD_PARAMS["thread/resume"].validate({"threadId": 1})


def test_params_enforce_bounds_and_defaults():
    with pytest.raises(ParamValidationError):
        METHOD_PARAMS["thread/list"].validate({"limit": 0})
    with pytest.raises(ParamValidationError):
        METHOD_PARAMS["thread/list"].validate({"limit": 201})
    values = METHOD_PARAMS["thread/list"].validate({})
    assert values == {"cursor": None, "limit": 50}
    with pytest.raises(ParamValidationError):
        METHOD_PARAMS["turn/start"].validate({"threadId": "t", "input": ""})


def test_initialize_validates_nested_client_info():
    with pytest.raises(ParamValidationError) as exc:
        METHOD_PARAMS["initialize"].validate(
            {"protocolVersion": "1.0", "clientInfo": {"name": ""}}
        )
    locs = [issue["loc"] for issue in exc.value.issues]
    assert ["clientInfo", "name"] in locs and ["clientInfo", "version"] in locs


# --- 端到端:socket + JSON-RPC --------------------------------------------


class _Client:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    async def send(self, payload):
        self.writer.write((json.dumps(payload) + "\n").encode())
        await self.writer.drain()

    async def recv(self):
        return json.loads(await self.reader.readline())

    async def handshake(self):
        await self.send(
            {
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {
                    "protocolVersion": "1.0",
                    "clientInfo": {"name": "test", "version": "1"},
                },
            }
        )
        result = await self.recv()
        await self.send({"jsonrpc": "2.0", "method": "initialized"})
        return result


async def _serve(workspace, store, executor=None):
    async def default_executor(request: TurnRequest) -> ControlExecutionResult:
        return ControlExecutionResult(response=f"echo:{request.input}")

    sessions = SessionManager(workspace)
    runtime = ConversationRuntime(store, executor or default_executor)
    service = ControlService(runtime, sessions, store, workspace, ready=lambda: True)
    server = SocketAppServer(workspace / "control.sock", service)
    await server.start()
    reader, writer = await asyncio.open_unix_connection(str(workspace / "control.sock"))
    return _Client(reader, writer), server, runtime, sessions


@pytest.mark.asyncio
async def test_requests_before_initialize_are_rejected(workspace, store):
    client, server, runtime, sessions = await _serve(workspace, store)
    try:
        await client.send({"jsonrpc": "2.0", "id": 1, "method": "server/status", "params": {}})
        assert (await client.recv())["error"]["code"] == -32002  # NOT_INITIALIZED
    finally:
        client.writer.close()
        await server.stop()
        await runtime.shutdown()
        sessions.close()


@pytest.mark.asyncio
async def test_unsupported_protocol_version_is_rejected(workspace, store):
    client, server, runtime, sessions = await _serve(workspace, store)
    try:
        await client.send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2.0",
                    "clientInfo": {"name": "t", "version": "1"},
                },
            }
        )
        error = (await client.recv())["error"]
        assert error["code"] == -32003  # INCOMPATIBLE_VERSION
        assert error["data"]["supported"] == ["1.0"]
    finally:
        client.writer.close()
        await server.stop()
        await runtime.shutdown()
        sessions.close()


@pytest.mark.asyncio
async def test_full_thread_and_turn_roundtrip(workspace, store):
    client, server, runtime, sessions = await _serve(workspace, store)
    try:
        assert (await client.handshake())["result"]["protocolVersion"] == "1.0"

        await client.send({"jsonrpc": "2.0", "id": 1, "method": "thread/start", "params": {}})
        thread_id = (await client.recv())["result"]["id"]
        assert thread_id.startswith("programmatic:")
        assert (await client.recv())["method"] == "thread/started"

        await client.send(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "turn/start",
                "params": {"threadId": thread_id, "input": "hi"},
            }
        )
        final = None
        while final is None:
            message = await client.recv()
            if message.get("method") == "turn/completed":
                final = message["params"]["turn"]
        assert final["status"] == "completed"
        assert final["finalResponse"] == "echo:hi"

        await client.send(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "thread/read",
                "params": {"threadId": thread_id, "includeTurns": True},
            }
        )
        payload = (await client.recv())["result"]
        assert len(payload["turns"]) == 1
    finally:
        client.writer.close()
        await server.stop()
        await runtime.shutdown()
        sessions.close()


@pytest.mark.asyncio
async def test_unknown_method_and_bad_params_map_to_standard_codes(workspace, store):
    client, server, runtime, sessions = await _serve(workspace, store)
    try:
        await client.handshake()
        await client.send({"jsonrpc": "2.0", "id": 1, "method": "does/not/exist", "params": {}})
        assert (await client.recv())["error"]["code"] == -32601
        await client.send(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "turn/start",
                "params": {"threadId": "programmatic:missing", "input": "x"},
            }
        )
        assert (await client.recv())["error"]["code"] == -32010  # THREAD_NOT_FOUND
        client.writer.write(b"{not json}\n")
        await client.writer.drain()
        assert (await client.recv())["error"]["code"] == -32700  # PARSE_ERROR
    finally:
        client.writer.close()
        await server.stop()
        await runtime.shutdown()
        sessions.close()


@pytest.mark.asyncio
async def test_socket_is_owner_only_and_refuses_second_owner(workspace, store):
    client, server, runtime, sessions = await _serve(workspace, store)
    try:
        mode = (workspace / "control.sock").stat().st_mode & 0o777
        assert mode == 0o600
        # 已有活跃 owner 时必须 fail loud,不能悄悄顶掉
        clash = SocketAppServer(workspace / "control.sock", server._service)
        with pytest.raises(RuntimeError):
            await clash.start()
    finally:
        client.writer.close()
        await server.stop()
        await runtime.shutdown()
        sessions.close()
