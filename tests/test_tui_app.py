"""Headless Textual smoke test, skipped when the optional UI is unavailable."""

import importlib.util
import unittest


TEXTUAL_AVAILABLE = importlib.util.find_spec("textual") is not None


class _FakeBus:
    def __init__(self):
        self.inbound = []

    def subscribe_outbound(self, _channel, _callback):
        return None

    def unsubscribe_outbound(self, _channel, _callback):
        return None

    async def publish_inbound(self, message):
        self.inbound.append(message)


class _FakeEventBus:
    def on(self, _event_type, _callback):
        return None

    def off(self, _event_type, _callback):
        return None


class _FakeLoop:
    def request_interrupt(self, _session_key):
        return True


class _FakeSession:
    def __init__(self, messages=None):
        self.messages = list(messages or [])


class _FakeSessionManager:
    def __init__(self):
        self.sessions = {
            "cli:local": _FakeSession(
                [
                    {"role": "user", "content": "saved question"},
                    {"role": "assistant", "content": "saved answer"},
                ]
            )
        }

    def get_or_create(self, key):
        return self.sessions.setdefault(key, _FakeSession())

    def list_sessions(self):
        return [
            {
                "key": key,
                "message_count": len(session.messages),
                "updated_at": "2026-07-20T12:00:00+09:00",
            }
            for key, session in self.sessions.items()
        ]


class _FakeRuntime:
    def __init__(self):
        self.bus = _FakeBus()
        self.event_bus = _FakeEventBus()
        self.loop = _FakeLoop()
        self.session_manager = _FakeSessionManager()
        self.stopped = False

    async def start_background(self, *, start_channels=False):
        self.start_channels = start_channels
        return []

    async def stop_background(self, _tasks):
        self.stopped = True


@unittest.skipUnless(TEXTUAL_AVAILABLE, "Textual is not installed")
class TuiAppTests(unittest.IsolatedAsyncioTestCase):
    async def test_headless_app_mounts_and_accepts_input(self):
        from pathlib import Path

        from frontend.tui.app import KirakiraTui

        runtime = _FakeRuntime()
        app = KirakiraTui(runtime, Path.cwd(), session_id="local")
        async with app.run_test(size=(100, 30)) as pilot:
            from textual.widgets import Input

            composer = app.query_one("#composer", Input)
            composer.value = "hello"
            await pilot.press("enter")
            await pilot.pause()
            self.assertEqual(runtime.bus.inbound[0].content, "hello")
            self.assertEqual(runtime.bus.inbound[0].chat_id, "local")
            self.assertFalse(runtime.start_channels)

    async def test_switches_named_session_without_sending_a_model_turn(self):
        from pathlib import Path

        from frontend.tui.app import KirakiraTui
        from textual.widgets import Input

        runtime = _FakeRuntime()
        app = KirakiraTui(runtime, Path.cwd(), session_id="local")
        async with app.run_test(size=(100, 30)) as pilot:
            composer = app.query_one("#composer", Input)
            composer.value = "/session project-x"
            await pilot.press("enter")
            await pilot.pause()
            self.assertEqual(app.state.session_key, "cli:project-x")
            self.assertEqual(runtime.bus.inbound, [])
            self.assertIn("cli:project-x", runtime.session_manager.sessions)

    async def test_default_is_fresh_and_sessions_picker_restores_history(self):
        from pathlib import Path

        from frontend.tui.app import KirakiraTui, SessionPicker
        from textual.widgets import Input

        runtime = _FakeRuntime()
        app = KirakiraTui(runtime, Path.cwd())
        async with app.run_test(size=(100, 30)) as pilot:
            self.assertTrue(app.state.session_key.startswith("cli:chat-"))
            self.assertEqual(
                runtime.session_manager.sessions[app.state.session_key].messages, []
            )
            composer = app.query_one("#composer", Input)
            composer.value = "/sessions"
            await pilot.press("enter")
            await pilot.pause()
            self.assertIsInstance(app.screen, SessionPicker)
            await pilot.press("enter")
            await pilot.pause()
            self.assertEqual(app.state.session_key, "cli:local")


if __name__ == "__main__":
    unittest.main()
