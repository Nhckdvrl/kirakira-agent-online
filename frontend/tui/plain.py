"""Line-oriented CLI with real lifecycle streaming and readline history."""

from __future__ import annotations

import asyncio
from pathlib import Path

from bus.events import InboundMessage, OutboundMessage
from bus.events_lifecycle import (
    ContextBudgetUpdated,
    ContextPrepared,
    StreamDeltaReady,
    ToolCallCompleted,
    ToolCallStarted,
    TurnFinished,
    TurnStarted,
)
from frontend.tui.state import TurnViewState, new_local_session_id

try:  # Importing readline enables arrows and persistent in-process history.
    import readline as _readline  # noqa: F401
except ImportError:  # pragma: no cover - Windows fallback
    _readline = None


async def runtime_plain_repl(
    runtime, workdir: Path, session_id: str | None = None
) -> None:
    session_id = str(session_id).strip() if session_id else new_local_session_id()
    session_key = "cli:%s" % session_id
    state = TurnViewState(session_key=session_key)
    finished = asyncio.Event()
    printed_by_iteration: dict[int, str] = {}
    line_open = False

    async def on_outbound(_msg: OutboundMessage) -> None:
        # TurnFinished is authoritative; this subscription only drains the CLI lane.
        return None

    async def on_started(event: TurnStarted) -> None:
        nonlocal line_open
        if not state.apply(event):
            return
        printed_by_iteration.clear()
        finished.clear()
        line_open = False

    async def on_delta(event: StreamDeltaReady) -> None:
        nonlocal line_open
        if not state.apply(event):
            return
        if event.content_delta:
            print(event.content_delta, end="", flush=True)
            printed_by_iteration[event.iteration] = (
                printed_by_iteration.get(event.iteration, "") + event.content_delta
            )
            line_open = True

    async def on_context(event: ContextPrepared) -> None:
        if not state.apply(event):
            return
        budget = "/%d" % event.input_budget if event.input_budget else ""
        print(
            "  · context %s · %d%s tokens · %d history"
            % (
                event.plan_name,
                event.estimated_tokens,
                budget,
                event.history_messages,
            ),
            flush=True,
        )

    async def on_context_budget(event: ContextBudgetUpdated) -> None:
        if not state.apply(event):
            return
        actual = event.model_usage.get("total_tokens")
        actual_text = " · %s actual" % actual if actual else ""
        print(
            "  · next context %d history tokens%s"
            % (event.history_tokens_estimate, actual_text),
            flush=True,
        )

    async def on_tool_started(event: ToolCallStarted) -> None:
        nonlocal line_open
        if not state.apply(event):
            return
        if line_open:
            print()
            line_open = False
        tool = state.tools[event.call_id]
        summary = state._arguments_summary(tool.arguments)
        print("  ◇ %s%s" % (tool.name, "  " + summary if summary else ""), flush=True)

    async def on_tool_completed(event: ToolCallCompleted) -> None:
        if not state.apply(event):
            return
        icon = "✓" if event.status == "success" else "✗"
        print("  %s %s  %s" % (icon, event.tool_name, event.status), flush=True)

    async def on_finished(event: TurnFinished) -> None:
        nonlocal line_open
        if not state.apply(event):
            return
        final = state.final_content
        streamed = printed_by_iteration.get(state.latest_iteration, "")
        if final:
            if final.startswith(streamed):
                suffix = final[len(streamed) :]
                if suffix:
                    print(suffix, end="", flush=True)
                    line_open = True
            elif final != streamed:
                if line_open:
                    print()
                print(final, end="", flush=True)
                line_open = True
        elif event.error:
            if line_open:
                print()
            print("Error: " + event.error, end="", flush=True)
            line_open = True
        if line_open:
            print()
        print("  · %.1fs · %s" % (event.duration_seconds, event.status), flush=True)
        line_open = False
        finished.set()

    runtime.bus.subscribe_outbound("cli", on_outbound)
    runtime.event_bus.on(TurnStarted, on_started)
    runtime.event_bus.on(StreamDeltaReady, on_delta)
    runtime.event_bus.on(ContextPrepared, on_context)
    runtime.event_bus.on(ContextBudgetUpdated, on_context_budget)
    runtime.event_bus.on(ToolCallStarted, on_tool_started)
    runtime.event_bus.on(ToolCallCompleted, on_tool_completed)
    runtime.event_bus.on(TurnFinished, on_finished)
    tasks = await runtime.start_background(start_channels=False)
    persisted = runtime.session_manager.get_or_create(session_key)
    print(
        "kirakira-agent ready · session %s · restored %d messages · /tools /skills /memory /exit"
        % (session_id, len(persisted.messages))
    )
    try:
        while True:
            try:
                query = (await asyncio.to_thread(input, "kirakira › ")).strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not query:
                continue
            if query in ("/exit", "exit", "q", "quit"):
                break
            await runtime.bus.publish_inbound(
                InboundMessage(
                    channel="cli",
                    sender="local",
                    chat_id=session_id,
                    content=query,
                )
            )
            await finished.wait()
    finally:
        runtime.bus.unsubscribe_outbound("cli", on_outbound)
        runtime.event_bus.off(TurnStarted, on_started)
        runtime.event_bus.off(StreamDeltaReady, on_delta)
        runtime.event_bus.off(ContextPrepared, on_context)
        runtime.event_bus.off(ContextBudgetUpdated, on_context_budget)
        runtime.event_bus.off(ToolCallStarted, on_tool_started)
        runtime.event_bus.off(ToolCallCompleted, on_tool_completed)
        runtime.event_bus.off(TurnFinished, on_finished)
        await runtime.stop_background(tasks)
