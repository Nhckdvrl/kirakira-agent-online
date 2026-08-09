"""Kirakira Agent learning harness module."""

import tempfile
import unittest
from pathlib import Path

from agent.core.runner import Agent
from core.schema import ModelResponse, ToolCall
from agent.tools import build_default_registry


class FakeModel:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def complete(self, messages, tools, system, model, max_tokens):
        self.calls.append(
            {
                "messages": list(messages),
                "tools": list(tools),
                "system": system,
                "model": model,
                "max_tokens": max_tokens,
            }
        )
        return self.responses.pop(0)


class AgentTests(unittest.TestCase):
    def test_agent_runs_tool_loop_then_final_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            (workdir / "README.md").write_text("hello")
            model = FakeModel(
                [
                    ModelResponse(tool_calls=[ToolCall("call_1", "read_file", {"path": "README.md"})]),
                    ModelResponse(text="Read it.", stop_reason="end_turn"),
                ]
            )
            agent = Agent(model, build_default_registry(workdir), "fake-model", workdir)
            messages = [{"role": "user", "content": "read README"}]

            response = agent.run(messages)

            self.assertEqual(response.text, "Read it.")
            self.assertEqual(len(model.calls), 2)
            self.assertTrue(any(msg.get("role") == "tool" and msg.get("content") == "hello" for msg in messages))

    def test_agent_returns_unknown_tool_as_tool_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            model = FakeModel(
                [
                    ModelResponse(tool_calls=[ToolCall("call_1", "nope", {})]),
                    ModelResponse(text="Handled.", stop_reason="end_turn"),
                ]
            )
            agent = Agent(model, build_default_registry(workdir), "fake-model", workdir)
            messages = [{"role": "user", "content": "call unknown"}]

            response = agent.run(messages)

            self.assertEqual(response.text, "Handled.")
            self.assertTrue(any("Unknown tool" in msg.get("content", "") for msg in messages))

    def test_agent_handles_no_tool_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            model = FakeModel([ModelResponse(text="done", stop_reason="end_turn")])
            agent = Agent(model, build_default_registry(workdir), "fake-model", workdir)

            response = agent.run([{"role": "user", "content": "hi"}])

            self.assertEqual(response.text, "done")


if __name__ == "__main__":
    unittest.main()
