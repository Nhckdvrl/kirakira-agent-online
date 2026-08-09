"""Kirakira Agent learning harness module."""

import json
import os
import unittest

from infra.providers.llm_provider import (
    OpenAICompatibleClient,
    _parse_tool_arguments,
)
from agent.model_runtime.types import ContextLengthError
from core.schema import ModelResponse, ToolCall, ToolResult, ToolSpec, assistant_message_from_response, tool_result_message


class OpenAICompatibleTests(unittest.TestCase):
    def test_tool_arguments_repair_matches_current_reference(self):
        self.assertEqual(
            _parse_tool_arguments('{"command":"echo ok'),
            {"command": "echo ok"},
        )
        with self.assertRaises(TypeError):
            _parse_tool_arguments('["not", "an", "object"]')
        with self.assertRaises(json.JSONDecodeError):
            _parse_tool_arguments("not json")

    def test_stream_parser_repairs_incomplete_tool_arguments(self):
        state = {
            "chunks": [],
            "text_parts": [],
            "reasoning_parts": [],
            "raw_calls": {
                0: {"id": "call_1", "name": "bash", "arguments": '{"command":"pwd'}
            },
            "finish_reason": "tool_calls",
        }
        client = OpenAICompatibleClient(base_url="http://example.test/v1", api_key="")

        response = client._finalize_stream(state)

        self.assertEqual(response.tool_calls[0].arguments, {"command": "pwd"})

    def test_preflight_rejects_oversized_context_before_network(self):
        client = OpenAICompatibleClient(
            base_url="http://example.test/v1",
            api_key="",
            context_window=100,
            effective_context_percent=0.9,
        )
        with self.assertRaises(ContextLengthError):
            client._build_payload(
                [{"role": "user", "content": "x" * 300}],
                [],
                "",
                "model",
                20,
            )

    def test_tool_choice_passes_through_payload(self):
        client = OpenAICompatibleClient(base_url="http://example.test/v1", api_key="")
        tools = [ToolSpec("finish_drift", "finish", {"type": "object", "properties": {}})]
        default = client._build_payload([{"role": "user", "content": "x"}], tools, "", "m", 20)
        self.assertEqual(default["tool_choice"], "auto")
        required = client._build_payload(
            [{"role": "user", "content": "x"}], tools, "", "m", 20, tool_choice="required"
        )
        self.assertEqual(required["tool_choice"], "required")
        named = {"type": "function", "function": {"name": "finish_drift"}}
        forced = client._build_payload(
            [{"role": "user", "content": "x"}], tools, "", "m", 20, tool_choice=named
        )
        self.assertEqual(forced["tool_choice"], named)
        # 无工具时不携带 tool_choice(与旧行为一致)
        bare = client._build_payload([{"role": "user", "content": "x"}], [], "", "m", 20)
        self.assertNotIn("tool_choice", bare)

    def test_stream_parser_accumulates_text_reasoning_and_fragmented_tool_call(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def __iter__(self):
                chunks = [
                    {"choices": [{"delta": {"reasoning_content": "think "}}]},
                    {"choices": [{"delta": {"content": "hello "}}]},
                    {
                        "choices": [
                            {
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "id": "call_1",
                                            "function": {
                                                "name": "read_file",
                                                "arguments": '{"path":',
                                            },
                                        }
                                    ]
                                }
                            }
                        ]
                    },
                    {
                        "choices": [
                            {
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "function": {"arguments": '"README.md"}'},
                                        }
                                    ]
                                },
                                "finish_reason": "tool_calls",
                            }
                        ]
                    },
                ]
                for chunk in chunks:
                    yield ("data: " + json.dumps(chunk) + "\n").encode("utf-8")
                yield b"data: [DONE]\n"

        client = OpenAICompatibleClient(base_url="http://example.test/v1", api_key="")
        client._open = lambda _payload: FakeResponse()
        deltas = []

        response = client.complete_stream(
            [{"role": "user", "content": "hi"}],
            [],
            "",
            "model",
            100,
            lambda content, reasoning: deltas.append((content, reasoning)),
        )

        self.assertEqual(response.text, "hello ")
        self.assertEqual(response.reasoning_content, "think ")
        self.assertEqual(response.tool_calls[0].name, "read_file")
        self.assertEqual(response.tool_calls[0].arguments, {"path": "README.md"})
        self.assertEqual(deltas, [("", "think "), ("hello ", "")])

    def test_parse_tool_call_response(self):
        client = OpenAICompatibleClient(base_url="http://example.test/v1", api_key="")
        payload = {
            "choices": [
                {
                    "finish_reason": "tool_calls",
                    "message": {
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "read_file",
                                    "arguments": json.dumps({"path": "README.md"}),
                                },
                            }
                        ],
                    },
                }
            ]
        }
        response = client.parse_response(payload)

        self.assertEqual(response.stop_reason, "tool_use")
        self.assertEqual(response.tool_calls[0].name, "read_file")
        self.assertEqual(response.tool_calls[0].arguments["path"], "README.md")

    def test_parse_and_forward_reasoning_content_for_tool_calls(self):
        client = OpenAICompatibleClient(base_url="http://example.test/v1", api_key="")
        payload = {
            "choices": [
                {
                    "finish_reason": "tool_calls",
                    "message": {
                        "content": "I will inspect the file.",
                        "reasoning_content": "Need to read README before answering.",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "read_file",
                                    "arguments": json.dumps({"path": "README.md"}),
                                },
                            }
                        ],
                    },
                }
            ]
        }

        response = client.parse_response(payload)
        converted = client._to_openai_messages([assistant_message_from_response(response)], system="")

        self.assertEqual(response.reasoning_content, "Need to read README before answering.")
        self.assertEqual(converted[0]["reasoning_content"], "Need to read README before answering.")

    def test_tool_result_message_shape(self):
        message = tool_result_message(ToolResult("call_1", "done", False))

        self.assertEqual(message["role"], "tool")
        self.assertEqual(message["tool_call_id"], "call_1")
        self.assertEqual(message["content"], "done")

    def test_to_openai_messages_serializes_tool_call_arguments(self):
        client = OpenAICompatibleClient(base_url="http://example.test/v1", api_key="")
        response = ModelResponse(tool_calls=[ToolCall("call_1", "bash", {"command": "pwd"})])
        messages = [assistant_message_from_response(response)]

        converted = client._to_openai_messages(messages, system="sys")

        self.assertEqual(converted[0]["role"], "system")
        args = converted[1]["tool_calls"][0]["function"]["arguments"]
        self.assertEqual(json.loads(args), {"command": "pwd"})

    def test_deepseek_v4_disables_thinking_by_default(self):
        client = OpenAICompatibleClient(base_url="https://api.deepseek.com", api_key="")

        self.assertEqual(client._thinking_config("deepseek-v4-flash"), {"type": "disabled"})

    def test_thinking_config_can_be_overridden(self):
        old_value = os.environ.get("OPENAI_COMPATIBLE_THINKING")
        os.environ["OPENAI_COMPATIBLE_THINKING"] = "enabled"
        try:
            client = OpenAICompatibleClient(base_url="https://api.deepseek.com", api_key="")
            self.assertEqual(client._thinking_config("deepseek-v4-flash"), {"type": "enabled"})
        finally:
            if old_value is None:
                os.environ.pop("OPENAI_COMPATIBLE_THINKING", None)
            else:
                os.environ["OPENAI_COMPATIBLE_THINKING"] = old_value


if __name__ == "__main__":
    unittest.main()
