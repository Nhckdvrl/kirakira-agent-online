"""Durable replay/ambiguity hooks for the unchanged ReAct tool loop."""

from __future__ import annotations

import hashlib
import json
from uuid import UUID

from agent.tool_hooks import HookContext, HookOutcome
from cloud.store import CloudStore


def _signature(tool_name: str, arguments: dict) -> str:
    raw = json.dumps(
        {"tool": tool_name, "arguments": arguments},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class CloudToolCheckpointHook:
    def __init__(self, store: CloudStore, event: str) -> None:
        if event not in {"pre_tool_use", "post_tool_use", "post_tool_error"}:
            raise ValueError(event)
        self._store = store
        self.event = event
        self.name = f"cloud-tool-checkpoint-{event}"

    def matches(self, ctx: HookContext) -> bool:
        return bool(ctx.request.chat_id)

    async def run(self, ctx: HookContext) -> HookOutcome:
        request = ctx.request
        run_id = UUID(request.chat_id)
        signature = _signature(request.tool_name, ctx.current_arguments)
        if self.event == "pre_tool_use":
            state, checkpoint = await self._store.begin_tool_checkpoint(
                run_id,
                iteration=request.iteration,
                call_index=request.call_index,
                signature=signature,
                tool_name=request.tool_name,
                arguments=ctx.current_arguments,
            )
            if state == "new":
                return HookOutcome()
            if state == "replay":
                return HookOutcome(
                    decision="replay",
                    replay_status=checkpoint.status,  # type: ignore[arg-type]
                    replay_output=checkpoint.output or "",
                    replay_mobile_attention=checkpoint.mobile_attention,
                )
            return HookOutcome(
                decision="abort",
                reason=(
                    "tool checkpoint diverged during retry"
                    if state == "diverged"
                    else "tool outcome is ambiguous after worker interruption; refusing duplicate execution"
                ),
            )

        output = str(ctx.result or "")
        await self._store.finish_tool_checkpoint(
            run_id,
            iteration=request.iteration,
            call_index=request.call_index,
            signature=signature,
            status="success" if self.event == "post_tool_use" else "error",
            output=output,
        )
        return HookOutcome()


def build_cloud_tool_checkpoint_hooks(store: CloudStore) -> list[CloudToolCheckpointHook]:
    return [
        CloudToolCheckpointHook(store, "pre_tool_use"),
        CloudToolCheckpointHook(store, "post_tool_use"),
        CloudToolCheckpointHook(store, "post_tool_error"),
    ]
