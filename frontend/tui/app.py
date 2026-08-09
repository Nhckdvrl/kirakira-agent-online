"""Full-screen Textual client for the Kirakira runtime."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import ClassVar

from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.theme import Theme
from textual.widgets import Footer, Header, Input, Markdown, OptionList, Static
from textual.widgets.option_list import Option

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


class TurnWidget(Vertical):
    """One assistant response whose process and answer update in place."""

    def compose(self) -> ComposeResult:
        yield Static("● Kirakira", classes="assistant-label")
        yield Static("", classes="process-trace")
        yield Markdown("", classes="assistant-answer")

    def render_state(self, state: TurnViewState) -> None:
        process = self.query_one(".process-trace", Static)
        answer = self.query_one(".assistant-answer", Markdown)
        trace = state.process_text()
        process.update(trace)
        process.display = bool(trace)
        answer.update(state.draft_answer or ("_正在思考…_" if state.active else ""))


class HistoryTurnWidget(Vertical):
    """A compact, immutable assistant turn restored from disk."""

    def __init__(self, content: str, tool_names: list[str] | None = None) -> None:
        super().__init__()
        self.content = content
        self.tool_names = tool_names or []

    def compose(self) -> ComposeResult:
        yield Static("Kirakira", classes="assistant-label history-label")
        if self.tool_names:
            yield Static(
                "  ✓ " + "  ·  ".join(self.tool_names),
                classes="process-trace history-trace",
            )
        yield Markdown(self.content or "_（空回复）_", classes="assistant-answer")


class SessionPicker(ModalScreen[str | None]):
    """Keyboard-first saved-session picker opened by /sessions."""

    BINDINGS = [Binding("escape", "cancel", "返回", show=False)]

    def __init__(self, sessions: list[dict], current_key: str) -> None:
        super().__init__()
        self._session_ids = [str(item["key"])[4:] for item in sessions]
        self._current_key = current_key
        self._sessions = sessions

    def compose(self) -> ComposeResult:
        options: list[Option] = []
        for index, item in enumerate(self._sessions):
            key = str(item["key"])
            marker = "●" if key == self._current_key else " "
            prompt = Text(
                "%s %-28s %4s 条   %s"
                % (
                    marker,
                    key[4:],
                    item.get("message_count", 0),
                    str(item.get("updated_at") or "")[:19],
                )
            )
            options.append(Option(prompt, id="session-%d" % index))
        with Vertical(id="session-picker-dialog"):
            yield Static("恢复历史会话", id="session-picker-title")
            yield OptionList(*options, id="session-options")
            yield Static("↑↓ 选择  ·  Enter 恢复  ·  Esc 返回", id="session-picker-help")

    def on_mount(self) -> None:
        for index, item in enumerate(self._sessions):
            if str(item["key"]) == self._current_key:
                self.query_one("#session-options", OptionList).highlighted = index
                break

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected
    ) -> None:
        raw = str(event.option.id or "")
        try:
            index = int(raw.rsplit("-", 1)[1])
        except (IndexError, ValueError):
            return
        self.dismiss(self._session_ids[index])

    def action_cancel(self) -> None:
        self.dismiss(None)


class KirakiraTui(App[None]):
    TITLE = "Kirakira Agent"
    SUB_TITLE = "local runtime"
    CSS = """
    Screen {
        background: #080908;
        color: #e7e7e3;
        layout: vertical;
    }

    Header {
        height: 1;
        background: #080908;
        color: #d7ddd7;
    }

    #conversation {
        height: 1fr;
        padding: 1 3;
        scrollbar-color: #3f5f47;
        scrollbar-background: #111311;
    }

    .welcome {
        color: #777d77;
        margin: 1 0 2 0;
        text-align: center;
    }

    .user-card {
        width: 82%;
        margin: 1 0 1 8;
        padding: 1 2;
        background: #171a17;
        color: #eeeeea;
        border: round #303530;
    }

    TurnWidget, HistoryTurnWidget {
        width: 92%;
        height: auto;
        margin: 1 8 2 0;
        padding: 0 1 1 1;
        background: #0d0f0d;
        border-left: wide #4f8a5d;
    }

    .assistant-label {
        height: 1;
        margin: 0 0 1 0;
        color: #8fc49a;
        text-style: bold;
    }

    .history-label {
        color: #768d7b;
    }

    .process-trace {
        height: auto;
        margin: 0 0 1 1;
        padding: 0 1;
        color: #8a918a;
        background: #101210;
        border-left: tall #293029;
    }

    .history-trace {
        color: #697169;
    }

    .assistant-answer {
        height: auto;
        padding: 0 1;
        color: #deded9;
        background: transparent;
    }

    #turn-status {
        height: 1;
        padding: 0 2;
        background: #0d0f0d;
        color: #718b77;
    }

    #composer {
        dock: bottom;
        height: 3;
        margin: 0 2 1 2;
        padding: 0 1;
        background: #121412;
        color: #eeeeea;
        border: round #323832;
    }

    #composer:focus {
        border: round #5b9768;
    }

    Footer {
        height: 1;
        background: #080908;
        color: #727772;
    }

    .session-notice {
        height: auto;
        margin: 1 3;
        padding: 1 2;
        color: #a4ada5;
        background: #111311;
        border-left: tall #3d6b47;
    }

    SessionPicker {
        align: center middle;
        background: #000000 70%;
    }

    #session-picker-dialog {
        width: 76;
        height: 70%;
        padding: 1 2;
        background: #101210;
        border: round #3d6b47;
    }

    #session-picker-title {
        height: 2;
        color: #dfe5df;
        text-style: bold;
    }

    #session-options {
        height: 1fr;
        background: #101210;
        color: #b8beb8;
        border: none;
    }

    #session-options > .option-list--option-highlighted {
        background: #243328;
        color: #eef4ee;
    }

    #session-picker-help {
        height: 2;
        padding-top: 1;
        color: #737b73;
        text-align: center;
    }
    """
    BINDINGS: ClassVar[list[Binding]] = [
        Binding("ctrl+c", "interrupt", "停止 / 退出", show=True, priority=True),
        Binding("ctrl+l", "clear_chat", "清屏", show=True),
        Binding("ctrl+q", "quit", "退出", show=True),
    ]

    HISTORY_VIEW_LIMIT = 40

    def __init__(
        self, runtime, workdir: Path, session_id: str | None = None
    ) -> None:
        super().__init__()
        self.register_theme(
            Theme(
                name="kirakira-charcoal",
                primary="#5b9768",
                secondary="#718b77",
                accent="#8fc49a",
                warning="#b4a269",
                error="#b96868",
                success="#69a878",
                foreground="#e7e7e3",
                background="#080908",
                surface="#0d0f0d",
                panel="#121412",
                dark=True,
            )
        )
        self.theme = "kirakira-charcoal"
        self.runtime = runtime
        self.workdir = workdir
        self._session_id = self._validate_session_id(
            session_id or new_local_session_id()
        )
        self.state = TurnViewState(session_key=self._session_key)
        self._runtime_tasks: list[asyncio.Task] = []
        self._turn_widget: TurnWidget | None = None
        self._history: list[str] = []
        self._history_index = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with VerticalScroll(id="conversation"):
            yield Static(
                "✦ Kirakira 已连接 · Enter 发送 · ↑↓ 历史 · Ctrl+C 中断",
                classes="welcome",
            )
        yield Static("● Ready", id="turn-status")
        yield Input(placeholder="和 Kirakira 说点什么…", id="composer")
        yield Footer()

    async def on_mount(self) -> None:
        self.runtime.bus.subscribe_outbound("cli", self._on_outbound)
        self.runtime.event_bus.on(TurnStarted, self._on_turn_started)
        self.runtime.event_bus.on(StreamDeltaReady, self._on_stream_delta)
        self.runtime.event_bus.on(ContextPrepared, self._on_context_prepared)
        self.runtime.event_bus.on(ContextBudgetUpdated, self._on_context_budget_updated)
        self.runtime.event_bus.on(ToolCallStarted, self._on_tool_started)
        self.runtime.event_bus.on(ToolCallCompleted, self._on_tool_completed)
        self.runtime.event_bus.on(TurnFinished, self._on_turn_finished)
        self._runtime_tasks = await self.runtime.start_background(start_channels=False)
        await self._restore_session_view()
        self.query_one("#composer", Input).focus()

    async def on_unmount(self) -> None:
        self.runtime.bus.unsubscribe_outbound("cli", self._on_outbound)
        self.runtime.event_bus.off(TurnStarted, self._on_turn_started)
        self.runtime.event_bus.off(StreamDeltaReady, self._on_stream_delta)
        self.runtime.event_bus.off(ContextPrepared, self._on_context_prepared)
        self.runtime.event_bus.off(ContextBudgetUpdated, self._on_context_budget_updated)
        self.runtime.event_bus.off(ToolCallStarted, self._on_tool_started)
        self.runtime.event_bus.off(ToolCallCompleted, self._on_tool_completed)
        self.runtime.event_bus.off(TurnFinished, self._on_turn_finished)
        if self._runtime_tasks:
            await self.runtime.stop_background(self._runtime_tasks)
            self._runtime_tasks = []

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        query = event.value.strip()
        if not query:
            return
        if query in ("/exit", "exit", "q", "quit"):
            self.exit()
            return
        if query == "/clear":
            await self.action_clear_chat()
            event.input.value = ""
            return
        if query == "/sessions":
            event.input.value = ""
            await self._open_session_picker()
            return
        if query == "/session" or query.startswith("/session "):
            event.input.value = ""
            requested = query[len("/session") :].strip()
            if not requested:
                self.notify("用法：/session <名称>", severity="warning")
                return
            try:
                await self._switch_session(requested)
            except ValueError as exc:
                self.notify(str(exc), severity="error")
            return
        if self.state.active:
            self.notify("当前任务仍在运行；按 Ctrl+C 可中断。", severity="warning")
            return
        self._history.append(query)
        self._history_index = len(self._history)
        event.input.value = ""
        # Close the small gap before TurnStarted arrives so a fast second Enter
        # cannot enqueue another turn for the same composer.
        self.state.reset(query)
        event.input.disabled = True
        conversation = self.query_one("#conversation", VerticalScroll)
        await conversation.mount(Static(Text(query), classes="user-card"))
        self._turn_widget = TurnWidget()
        await conversation.mount(self._turn_widget)
        self.query_one("#turn-status", Static).update("◌ Queued")
        conversation.scroll_end(animate=False)
        try:
            await self.runtime.bus.publish_inbound(
                InboundMessage(
                    channel="cli",
                    sender="local",
                    chat_id=self._session_id,
                    content=query,
                )
            )
        except Exception:
            self.state.active = False
            event.input.disabled = False
            event.input.focus()
            raise

    async def on_key(self, event: events.Key) -> None:
        composer = self.query_one("#composer", Input)
        if not composer.has_focus or self.state.active:
            return
        if event.key == "up" and self._history:
            self._history_index = max(0, self._history_index - 1)
            composer.value = self._history[self._history_index]
            composer.cursor_position = len(composer.value)
            event.prevent_default()
            event.stop()
        elif event.key == "down" and self._history:
            self._history_index = min(len(self._history), self._history_index + 1)
            composer.value = (
                "" if self._history_index == len(self._history) else self._history[self._history_index]
            )
            composer.cursor_position = len(composer.value)
            event.prevent_default()
            event.stop()

    async def _on_outbound(self, _msg: OutboundMessage) -> None:
        # TurnFinished owns final rendering; subscribing drains the outbound lane.
        return None

    async def _on_turn_started(self, event: TurnStarted) -> None:
        if self.state.apply(event):
            self.query_one("#turn-status", Static).update("◌ Thinking · iteration 1")
            self._render_turn()

    async def _on_stream_delta(self, event: StreamDeltaReady) -> None:
        if self.state.apply(event):
            self.query_one("#turn-status", Static).update(
                "◌ Streaming · iteration %d" % (event.iteration + 1)
            )
            self._render_turn()

    async def _on_context_prepared(self, event: ContextPrepared) -> None:
        if self.state.apply(event):
            self.query_one("#turn-status", Static).update(
                "◌ Context · %s · %s tokens"
                % (event.plan_name, self.state._compact_number(event.estimated_tokens))
            )
            self._render_turn()

    async def _on_context_budget_updated(self, event: ContextBudgetUpdated) -> None:
        if self.state.apply(event):
            self._render_turn()

    async def _on_tool_started(self, event: ToolCallStarted) -> None:
        if self.state.apply(event):
            self.query_one("#turn-status", Static).update("◇ Running · " + event.tool_name)
            self._render_turn()

    async def _on_tool_completed(self, event: ToolCallCompleted) -> None:
        if self.state.apply(event):
            icon = "✓" if event.status == "success" else "✗"
            self.query_one("#turn-status", Static).update(
                "%s %s · %s" % (icon, event.tool_name, event.status)
            )
            self._render_turn()

    async def _on_turn_finished(self, event: TurnFinished) -> None:
        if not self.state.apply(event):
            return
        icon = {"success": "●", "interrupted": "■", "error": "✗"}.get(
            event.status, "●"
        )
        self.query_one("#turn-status", Static).update(
            "%s %s · %.1fs" % (icon, event.status.title(), event.duration_seconds)
        )
        self._render_turn()
        composer = self.query_one("#composer", Input)
        composer.disabled = False
        composer.focus()
        self.query_one("#conversation", VerticalScroll).scroll_end(animate=False)

    def _render_turn(self) -> None:
        if self._turn_widget is not None and self._turn_widget.is_mounted:
            self._turn_widget.render_state(self.state)
            self.call_after_refresh(self._scroll_to_end)

    def _scroll_to_end(self) -> None:
        self.query_one("#conversation", VerticalScroll).scroll_end(animate=False)

    def action_interrupt(self) -> None:
        if self.state.active:
            interrupted = self.runtime.loop.request_interrupt(self._session_key)
            if interrupted:
                self.notify("正在中断当前任务…")
            return
        self.exit()

    async def action_clear_chat(self) -> None:
        conversation = self.query_one("#conversation", VerticalScroll)
        await conversation.remove_children()
        await conversation.mount(
            Static(
                "视图已清空 · session %s 的磁盘记录仍保留" % self._session_id,
                classes="welcome",
            )
        )
        self._turn_widget = None

    @property
    def _session_key(self) -> str:
        return "cli:%s" % self._session_id

    @staticmethod
    def _validate_session_id(value: str) -> str:
        session_id = str(value).strip()
        if not session_id:
            raise ValueError("session 名称不能为空")
        if len(session_id) > 96:
            raise ValueError("session 名称不能超过 96 个字符")
        if any(ord(char) < 32 for char in session_id):
            raise ValueError("session 名称不能包含控制字符")
        return session_id

    async def _switch_session(self, session_id: str) -> None:
        if self.state.active:
            self.notify("当前任务仍在运行，完成或中断后再切换。", severity="warning")
            return
        self._session_id = self._validate_session_id(session_id)
        self.state = TurnViewState(session_key=self._session_key)
        await self._restore_session_view()
        self.notify("已切换到 session：%s" % self._session_id)

    async def _restore_session_view(self) -> None:
        conversation = self.query_one("#conversation", VerticalScroll)
        session = self.runtime.session_manager.get_or_create(self._session_key)
        messages = list(session.messages)
        visible = messages[-self.HISTORY_VIEW_LIMIT :]
        omitted = len(messages) - len(visible)
        await conversation.remove_children()
        widgets: list[Static | HistoryTurnWidget] = [
            Static(
                "session: %s  ·  已恢复 %d 条消息%s"
                % (
                    self._session_id,
                    len(visible),
                    "  ·  更早 %d 条仍保存在磁盘" % omitted if omitted else "",
                ),
                classes="welcome",
            )
        ]
        for message in visible:
            role = str(message.get("role") or "")
            content = str(message.get("content") or "")
            if role == "user":
                widgets.append(Static(Text(content), classes="user-card"))
            elif role == "assistant":
                widgets.append(
                    HistoryTurnWidget(content, self._stored_tool_names(message))
                )
        await conversation.mount(*widgets)
        self._turn_widget = None
        self.sub_title = "session · %s" % self._session_id
        self.query_one("#turn-status", Static).update(
            "● Ready · %s · %d messages" % (self._session_id, len(messages))
        )
        composer = self.query_one("#composer", Input)
        composer.placeholder = "发送到 %s…" % self._session_id
        conversation.scroll_end(animate=False)

    async def _open_session_picker(self) -> None:
        sessions = [
            item
            for item in self.runtime.session_manager.list_sessions()
            if str(item.get("key") or "").startswith("cli:")
            and int(item.get("message_count") or 0) > 0
        ]
        if not sessions:
            self.notify("还没有可恢复的历史会话。")
            return
        await self.push_screen(
            SessionPicker(sessions[:50], self._session_key),
            callback=self._on_session_picked,
        )

    async def _on_session_picked(self, session_id: str | None) -> None:
        if session_id:
            await self._switch_session(session_id)

    @staticmethod
    def _stored_tool_names(message: dict) -> list[str]:
        names: list[str] = []
        for group in message.get("tool_chain") or []:
            for call in group.get("calls") or []:
                name = str(call.get("name") or "")
                if name and name not in names:
                    names.append(name)
        return names


async def runtime_tui(
    runtime, workdir: Path, session_id: str | None = None
) -> None:
    await KirakiraTui(runtime, workdir, session_id=session_id).run_async()
