"""TurnResult 副作用提交契约(照 Reference agent/turns/)。

kirakira 此前把"成功写 Session / 失败保留未读"这类分支手写在主动与 Drift 里。
本抽象把它变成显式声明,这里锁定它的执行语义:
- 通用副作用先于投递(用于发送前落地状态);
- 成功/失败副作用只在对应分支跑;
- 单个副作用失败不阻断其余,也不改变投递结论。
"""

from __future__ import annotations

import asyncio
import unittest

from agent.turns.result import (
    CallableSideEffect,
    OutboundDispatch,
    TurnOutbound,
    TurnResult,
    commit_turn_result,
)


class _Port:
    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.calls: list[OutboundDispatch] = []

    async def dispatch(self, outbound: OutboundDispatch) -> bool:
        self.calls.append(outbound)
        return self.ok


def _effect(name: str, log: list[str], *, boom: bool = False) -> CallableSideEffect:
    async def action() -> None:
        if boom:
            raise RuntimeError("effect blew up: %s" % name)
        log.append(name)

    return CallableSideEffect(name=name, action=action)


class TurnCommitTests(unittest.TestCase):
    def test_success_path_runs_common_then_success_effects(self) -> None:
        async def scenario() -> None:
            log: list[str] = []
            result = TurnResult(
                decision="reply",
                outbound=TurnOutbound(session_key="cli:1", content="hi"),
                side_effects=[_effect("pre", log)],
                success_side_effects=[_effect("on_success", log)],
                failure_side_effects=[_effect("on_failure", log)],
            )
            port = _Port(ok=True)
            outcome = await commit_turn_result(
                result, port=port, channel="cli", chat_id="1"
            )
            self.assertTrue(outcome.dispatched)
            # 通用副作用先于投递,成功分支不跑失败副作用
            self.assertEqual(log, ["pre", "on_success"])
            self.assertEqual(port.calls[0].content, "hi")

        asyncio.run(scenario())

    def test_failed_dispatch_runs_failure_effects_only(self) -> None:
        async def scenario() -> None:
            log: list[str] = []
            result = TurnResult(
                decision="reply",
                outbound=TurnOutbound(session_key="cli:1", content="hi"),
                side_effects=[_effect("pre", log)],
                success_side_effects=[_effect("on_success", log)],
                failure_side_effects=[_effect("on_failure", log)],
            )
            outcome = await commit_turn_result(result, port=_Port(ok=False))
            self.assertFalse(outcome.dispatched)
            self.assertEqual(log, ["pre", "on_failure"])

        asyncio.run(scenario())

    def test_skip_decision_does_not_dispatch(self) -> None:
        async def scenario() -> None:
            log: list[str] = []
            port = _Port(ok=True)
            result = TurnResult(
                decision="skip",
                outbound=None,
                side_effects=[_effect("pre", log)],
                success_side_effects=[_effect("on_success", log)],
                failure_side_effects=[_effect("on_failure", log)],
            )
            outcome = await commit_turn_result(result, port=port)
            self.assertFalse(outcome.dispatched)
            self.assertEqual(port.calls, [])
            # skip 不是失败:没有尝试投递,不跑 failure 回滚(照 Reference turns/orchestrator.py)
            self.assertEqual(log, ["pre"])

        asyncio.run(scenario())

    def test_reply_without_outbound_runs_failure_rollback(self) -> None:
        async def scenario() -> None:
            log: list[str] = []
            result = TurnResult(
                decision="reply",
                outbound=None,
                side_effects=[_effect("pre", log)],
                failure_side_effects=[_effect("on_failure", log)],
            )
            outcome = await commit_turn_result(result, port=_Port(ok=True))
            # decision=reply 却没有产物是调用方缺陷,按投递失败清理预置状态
            self.assertFalse(outcome.dispatched)
            self.assertEqual(log, ["pre", "on_failure"])

        asyncio.run(scenario())

    def test_one_failing_effect_does_not_block_others_or_change_dispatch(self) -> None:
        async def scenario() -> None:
            log: list[str] = []
            result = TurnResult(
                decision="reply",
                outbound=TurnOutbound(session_key="cli:1", content="hi"),
                success_side_effects=[
                    _effect("first", log),
                    _effect("bad", log, boom=True),
                    _effect("last", log),
                ],
            )
            outcome = await commit_turn_result(result, port=_Port(ok=True))
            # 投递结论由渠道决定,不因副作用失败翻转
            self.assertTrue(outcome.dispatched)
            self.assertEqual(log, ["first", "last"])
            self.assertEqual(outcome.failures, ["bad"])
            self.assertEqual(outcome.ran, ["first", "last"])

        asyncio.run(scenario())

    def test_no_effects_is_clean(self) -> None:
        async def scenario() -> None:
            result = TurnResult(
                decision="reply",
                outbound=TurnOutbound(session_key="cli:1", content="hi"),
            )
            outcome = await commit_turn_result(result, port=_Port(ok=True))
            self.assertTrue(outcome.dispatched)
            self.assertEqual(outcome.ran, [])
            self.assertEqual(outcome.failures, [])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
