"""Kirakira Agent learning harness module."""

import asyncio
import json
from pathlib import Path
from typing import List, Optional

from agent.model_runtime.context import compact_messages, estimate_tokens, microcompact
from agent.model_runtime.types import ModelClient
from core.schema import JsonDict, ModelResponse, assistant_message_from_response, tool_result_message
from agent.tools.registry import ToolRegistry


DEFAULT_SYSTEM = (
    "You are a coding agent. You can use tools to solve tasks. "
    "Act when tool use helps, and give a concise final answer when done."
)


class Agent:
    def __init__(
        self,
        model_client: ModelClient,
        tool_registry: ToolRegistry,
        model: str,
        workdir: Path,
        system: str = DEFAULT_SYSTEM,
        max_tokens: int = 8000,
        token_threshold: int = 100000,
        persist_transcripts: bool = True,
    ) -> None:
        self.model_client = model_client
        self.tool_registry = tool_registry
        self.model = model
        self.workdir = workdir
        self.system = system
        self.max_tokens = max_tokens
        self.token_threshold = token_threshold
        self.transcript_dir = workdir / ".transcripts"
        self.persist_transcripts = persist_transcripts

    def run(
        self,
        messages: List[JsonDict],
        max_rounds: int = 50,
        tool_choice: object | None = None,
        final_tool_choice: object | None = None,
        stop_tools: frozenset[str] | set[str] = frozenset(),
    ) -> ModelResponse:
        return asyncio.run(
            self.arun(
                messages,
                max_rounds=max_rounds,
                tool_choice=tool_choice,
                final_tool_choice=final_tool_choice,
                stop_tools=stop_tools,
            )
        )

    async def arun(
        self,
        messages: List[JsonDict],
        max_rounds: int = 50,
        tool_choice: object | None = None,
        final_tool_choice: object | None = None,
        stop_tools: frozenset[str] | set[str] = frozenset(),
    ) -> ModelResponse:
        """ReAct 循环。

        `tool_choice`/`final_tool_choice`/`stop_tools` 一起复刻 Reference drift 主循环的
        收尾语义(plugins/drift_flow/runtime.py):每步可要求 "required"(必须调工具),
        最后一步具名强制收尾工具,收尾工具执行后立即结束(Reference 的 mandatory_exit_tools)。
        不传时行为与旧签名完全一致——kwargs 只在启用时才传给 model_client,旧测试替身不受影响。
        """
        last_response: Optional[ModelResponse] = None
        stop_names = frozenset(stop_tools)
        for round_index in range(max_rounds):
            microcompact(messages)
            if estimate_tokens(messages) > self.token_threshold:
                messages[:] = await self.acompact(messages)

            effective_choice = tool_choice
            if final_tool_choice is not None and round_index == max_rounds - 1:
                effective_choice = final_tool_choice
            kwargs: dict = {}
            if effective_choice is not None:
                kwargs["tool_choice"] = effective_choice
            acomplete = getattr(self.model_client, "acomplete", None)
            if callable(acomplete):
                response = await acomplete(
                    messages=messages,
                    tools=self.tool_registry.specs(),
                    system=self.system,
                    model=self.model,
                    max_tokens=self.max_tokens,
                    **kwargs,
                )
            else:
                response = await asyncio.to_thread(
                    self.model_client.complete,
                    messages=messages,
                    tools=self.tool_registry.specs(),
                    system=self.system,
                    model=self.model,
                    max_tokens=self.max_tokens,
                    **kwargs,
                )
            last_response = response
            messages.append(assistant_message_from_response(response))

            if not response.has_tool_calls:
                return response

            requested_compact = False
            stop_requested = False
            for call in response.tool_calls:
                result = await self.tool_registry.execute_async(call)
                if call.name == "compact":
                    requested_compact = True
                if call.name in stop_names:
                    stop_requested = True
                messages.append(tool_result_message(result))

            if requested_compact:
                messages[:] = await self.acompact(messages)
                return ModelResponse(text="Context compacted.", stop_reason="end_turn")
            if stop_requested:
                # 收尾工具已执行:立即结束,不再让模型多跑一轮
                return response

        return last_response or ModelResponse(text="", stop_reason="max_rounds")

    def compact(self, messages: List[JsonDict]) -> List[JsonDict]:
        return asyncio.run(self.acompact(messages))

    async def acompact(self, messages: List[JsonDict]) -> List[JsonDict]:
        transcript_text = json.dumps(messages, ensure_ascii=False, default=str)

        if self.persist_transcripts:
            # Preserve the Local adapter's transcript archive contract. Cloud
            # callers disable this and keep durable state in PostgreSQL.
            transcript_dir = self.transcript_dir
        else:
            transcript_dir = None

        async def summarize_async(text: str) -> str:
            prompt = (
                "Summarize this agent conversation for continuity. "
                "Preserve goals, decisions, changed files, open tasks, and important tool results.\n\n"
                + text
            )
            kwargs = {
                "messages": [{"role": "user", "content": prompt}],
                "tools": [],
                "system": "You compress agent transcripts into concise continuity summaries.",
                "model": self.model,
                "max_tokens": min(self.max_tokens, 2000),
            }
            acomplete = getattr(self.model_client, "acomplete", None)
            response = (
                await acomplete(**kwargs)
                if callable(acomplete)
                else await asyncio.to_thread(self.model_client.complete, **kwargs)
            )
            return response.text or "Conversation compressed."

        summary = await summarize_async(transcript_text[-80000:])
        if transcript_dir is not None:
            return compact_messages(messages, transcript_dir, summary=summary)
        return [{"role": "user", "content": "[Compressed]\n" + summary}]
