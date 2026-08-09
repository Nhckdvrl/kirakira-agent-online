"""agent_restart 链路(照 Reference agent/restart.py + tests/test_agent_restart.py)。

supervisor 侧(字节对齐的 agent/supervisor.py)只认:退出码 75 + 私有管道上
恰好一帧带本 boot nonce 的 restart_commit。本文件验证进程内的另一半:
- 提交需要 caller turn 正式终态 **且** 传输送达两个条件;
- 任一环节失败(turn 非 completed / 送达失败 / watchdog 超时)恢复准入;
- ConversationRuntime 只在 caller 是唯一在途 turn 时允许冻结;
- 提交帧能通过 supervisor 自己的 _valid_commit 校验(两半互认)。
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path

from agent.control.context import current_turn_id
from agent.control.errors import RuntimeClosedError
from agent.control.models import TurnRequest
from agent.control.ports import ControlExecutionResult
from agent.control.runtime import ConversationRuntime
from agent.control.store import ControlStore
from agent.restart import (
    RestartCoordinator,
    RestartRejectedError,
    RestartState,
    SupervisorCommitChannel,
)
from agent.supervisor import _valid_commit

NONCE = "n" * 64


def _coordinator(commits: list | None = None, **kwargs) -> RestartCoordinator:
    sink = commits if commits is not None else []
    coordinator = RestartCoordinator(
        "boot-1", supervised=True, commit=sink.append, **kwargs
    )
    return coordinator


def _bind_pass_through(coordinator: RestartCoordinator) -> dict:
    calls = {"quiesce": [], "resume": []}
    coordinator.bind_admission(
        quiesce=lambda turn_id: calls["quiesce"].append(turn_id),
        resume=lambda turn_id: calls["resume"].append(turn_id),
    )
    return calls


class CoordinatorTests(unittest.TestCase):
    def test_commit_requires_terminal_and_delivery_in_any_order(self) -> None:
        async def scenario() -> None:
            commits: list = []
            coordinator = _coordinator(commits)
            _bind_pass_through(coordinator)
            request = coordinator.arm(
                turn_id="t1", session_key="s", channel="c", chat_id="u", reason="升级"
            )
            # 只有终态:不提交
            coordinator.mark_turn_terminal("t1", "completed")
            self.assertEqual(commits, [])
            self.assertIs(coordinator.state, RestartState.WAITING_DELIVERY)
            # 送达补上:提交
            coordinator.mark_delivered("t1")
            self.assertEqual([c.id for c in commits], [request.id])
            self.assertIs(coordinator.state, RestartState.COMMITTED)
            got = await asyncio.wait_for(coordinator.wait_committed(), timeout=1)
            self.assertEqual(got.id, request.id)

        asyncio.run(scenario())

    def test_failed_turn_and_delivery_failure_restore_admission(self) -> None:
        async def scenario() -> None:
            for mark in (
                lambda c: c.mark_turn_terminal("t1", "failed"),
                lambda c: c.mark_delivery_failed("t1", "socket closed"),
            ):
                coordinator = _coordinator()
                calls = _bind_pass_through(coordinator)
                coordinator.arm(
                    turn_id="t1", session_key="s", channel="c", chat_id="u", reason="x"
                )
                mark(coordinator)
                self.assertIs(coordinator.state, RestartState.CANCELLED)
                self.assertIsNone(coordinator.pending)
                self.assertEqual(calls["resume"], ["t1"])

        asyncio.run(scenario())

    def test_delivery_watchdog_timeout_cancels_and_resumes(self) -> None:
        async def scenario() -> None:
            coordinator = _coordinator(delivery_timeout_s=0.05)
            calls = _bind_pass_through(coordinator)
            coordinator.arm(
                turn_id="t1", session_key="s", channel="c", chat_id="u", reason="x"
            )
            coordinator.mark_turn_terminal("t1", "completed")
            await asyncio.sleep(0.15)
            self.assertIs(coordinator.state, RestartState.CANCELLED)
            self.assertIn("timed out", coordinator.last_error or "")
            self.assertEqual(calls["resume"], ["t1"])

        asyncio.run(scenario())

    def test_same_caller_idempotent_other_caller_rejected(self) -> None:
        async def scenario() -> None:
            coordinator = _coordinator()
            _bind_pass_through(coordinator)
            first = coordinator.arm(
                turn_id="t1", session_key="s", channel="c", chat_id="u", reason="x"
            )
            again = coordinator.arm(
                turn_id="t1", session_key="s", channel="c", chat_id="u", reason="x"
            )
            self.assertIs(first, again)
            with self.assertRaises(RestartRejectedError):
                coordinator.arm(
                    turn_id="t2", session_key="s", channel="c", chat_id="u", reason="x"
                )

        asyncio.run(scenario())

    def test_unsupervised_and_missing_context_rejected(self) -> None:
        async def scenario() -> None:
            bare = RestartCoordinator("boot-1", supervised=False)
            bare.bind_admission(quiesce=lambda _t: None, resume=lambda _t: None)
            with self.assertRaises(RestartRejectedError):
                bare.arm(
                    turn_id="t1", session_key="s", channel="c", chat_id="u", reason="x"
                )
            coordinator = _coordinator()
            _bind_pass_through(coordinator)
            with self.assertRaises(RestartRejectedError):
                coordinator.arm(
                    turn_id="", session_key="s", channel="c", chat_id="u", reason="x"
                )

        asyncio.run(scenario())

    def test_supervised_requires_commit_channel(self) -> None:
        with self.assertRaises(ValueError):
            RestartCoordinator("boot-1", supervised=True, commit=None)
        with self.assertRaises(ValueError):
            RestartCoordinator("boot-1", supervised=False, commit=lambda _r: None)


class CommitChannelTests(unittest.TestCase):
    def test_commit_frame_passes_supervisor_validation(self) -> None:
        """两半互认:进程内写出的帧,必须被字节对齐的 supervisor 校验器接受。"""
        read_fd, write_fd = os.pipe()
        try:
            channel = SupervisorCommitChannel(write_fd, "boot-1", NONCE)
            coordinator = RestartCoordinator(
                "boot-1", supervised=True, commit=channel.commit
            )
            _bind_pass_through(coordinator)

            async def scenario() -> None:
                coordinator.arm(
                    turn_id="t1", session_key="s", channel="c", chat_id="u", reason="x"
                )
                coordinator.mark_turn_terminal("t1", "completed")
                coordinator.mark_delivered("t1")

            asyncio.run(scenario())
            payload = os.read(read_fd, 4096)
            self.assertTrue(_valid_commit(payload, boot_id="boot-1", nonce=NONCE))
            frame = json.loads(payload.splitlines()[0])
            self.assertEqual(frame["type"], "restart_commit")
            self.assertTrue(frame["requestId"].startswith("restart_"))
        finally:
            os.close(read_fd)
            try:
                os.close(write_fd)
            except OSError:
                pass

    def test_from_environment_missing_fd_fails_loud(self) -> None:
        original = dict(os.environ)
        try:
            for key in list(os.environ):
                if key.startswith(("AKASHIC_", "KIRAKIRA_")):
                    del os.environ[key]
            os.environ["AKASHIC_SUPERVISED"] = "1"
            with self.assertRaises(RuntimeError):
                SupervisorCommitChannel.from_environment()
            del os.environ["AKASHIC_SUPERVISED"]
            self.assertIsNone(SupervisorCommitChannel.from_environment())
        finally:
            os.environ.clear()
            os.environ.update(original)


class ConversationRuntimeAdmissionTests(unittest.TestCase):
    """quiesce:caller 必须是唯一在途 turn;冻结期间 start_turn 明确拒绝;取消后恢复。"""

    def _runtime(self, tmp: str, executor, coordinator=None) -> ConversationRuntime:
        store = ControlStore(Path(tmp) / "control.db")
        return ConversationRuntime(
            store, executor, restart_coordinator=coordinator
        )

    def test_restart_from_turn_freezes_admission_until_committed(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                commits: list = []
                coordinator = _coordinator(commits)
                armed: dict = {}

                async def executor(request: TurnRequest) -> ControlExecutionResult:
                    # 模拟 agent_restart 工具:用 ContextVar 自证 turn 身份
                    turn_id = current_turn_id.get()
                    request_obj = coordinator.arm(
                        turn_id=turn_id,
                        session_key=request.thread_id,
                        channel="control",
                        chat_id=request.thread_id,
                        reason="重载核心配置",
                    )
                    armed["turn_id"] = turn_id
                    armed["request_id"] = request_obj.id
                    return ControlExecutionResult("重启已安排")

                runtime = self._runtime(tmp, executor, coordinator)
                coordinator.bind_admission(
                    quiesce=runtime.quiesce_for_restart,
                    resume=runtime.resume_after_restart_cancel,
                )
                handle = await runtime.start_turn(
                    TurnRequest("programmatic:x", "请重启", {})
                )
                result = await handle.result()
                self.assertEqual(result.status.value, "completed")
                # arm 已冻结准入:新 turn 被拒
                with self.assertRaises(RuntimeClosedError):
                    await runtime.start_turn(TurnRequest("programmatic:y", "hi", {}))
                # 终态已由 runtime 上报;补送达确认 → 提交
                coordinator.mark_delivered(armed["turn_id"])
                self.assertEqual([c.id for c in commits], [armed["request_id"]])
                request_obj = await asyncio.wait_for(
                    coordinator.wait_committed(), timeout=1
                )
                self.assertEqual(request_obj.turn_id, armed["turn_id"])
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_quiesce_rejected_when_other_turn_in_flight(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                release = asyncio.Event()

                async def executor(request: TurnRequest) -> ControlExecutionResult:
                    if request.thread_id == "programmatic:slow":
                        await release.wait()
                    return ControlExecutionResult("ok")

                runtime = self._runtime(tmp, executor)
                slow = await runtime.start_turn(
                    TurnRequest("programmatic:slow", "hold", {})
                )
                await asyncio.sleep(0)
                # 另一个 turn 想冻结:被拒,因为 slow 仍在途
                with self.assertRaises(RuntimeClosedError):
                    runtime.quiesce_for_restart("some-other-turn")
                release.set()
                await slow.result()
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_cancelled_restart_restores_admission(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                coordinator = _coordinator(delivery_timeout_s=0.05)

                async def executor(request: TurnRequest) -> ControlExecutionResult:
                    if request.input == "arm":
                        coordinator.arm(
                            turn_id=current_turn_id.get(),
                            session_key=request.thread_id,
                            channel="control",
                            chat_id=request.thread_id,
                            reason="x",
                        )
                    return ControlExecutionResult("ok")

                runtime = self._runtime(tmp, executor, coordinator)
                coordinator.bind_admission(
                    quiesce=runtime.quiesce_for_restart,
                    resume=runtime.resume_after_restart_cancel,
                )
                handle = await runtime.start_turn(
                    TurnRequest("programmatic:x", "arm", {})
                )
                await handle.result()
                # 送达迟迟不来 → watchdog 取消并恢复准入
                await asyncio.sleep(0.15)
                self.assertIs(coordinator.state, RestartState.CANCELLED)
                follow_up = await runtime.start_turn(
                    TurnRequest("programmatic:x", "hello", {})
                )
                result = await follow_up.result()
                self.assertEqual(result.status.value, "completed")
                await runtime.shutdown()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
