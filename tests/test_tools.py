"""Kirakira Agent learning harness module."""

import tempfile
import unittest
from unittest import mock
import asyncio
import gzip
import json
import os
import socket
import shlex
import sys
import threading
import time
import urllib.parse
import urllib.error
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from bus.queue import MessageBus
from bus.events import OutboundMessage
from core.schema import ToolCall, ToolSpec
from agent.skills import SkillLoader
from agent.tools.builtins import WorkspaceTools, build_default_registry, safe_path
from agent.tools.registry import ToolRegistry
from agent.tool_hooks import ToolExecutionRequest, ToolExecutor


def _exa_sse(payload: dict) -> str:
    """把一帧 JSON-RPC 结果包成 Exa MCP 的 SSE 响应体。"""
    return "event: message\ndata: %s\n\n" % json.dumps(payload, ensure_ascii=False)


class _FakeHttpxResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _FakeHttpxClient:
    """替身 httpx.Client：要么返回预置 SSE，要么在 post 时抛出。"""

    def __init__(self, *, text: str | None = None, exc: Exception | None = None) -> None:
        self._text = text
        self._exc = exc

    def __enter__(self) -> "_FakeHttpxClient":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def post(self, *_args: object, **_kwargs: object) -> _FakeHttpxResponse:
        if self._exc is not None:
            raise self._exc
        return _FakeHttpxResponse(self._text or "")


class ToolTests(unittest.TestCase):
    def test_safe_path_blocks_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                safe_path(Path(tmp), "../outside.txt")

    def test_file_tools(self):
        with tempfile.TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            tools = WorkspaceTools(workdir, SkillLoader(workdir / "skills"))

            self.assertIn("Wrote", tools.write_file("a.txt", "hello\nworld"))
            self.assertEqual(tools.read_file("a.txt"), "hello\nworld")
            self.assertIn("file\ta.txt", tools.list_dir("."))
            self.assertIn("Edited", tools.edit_file("a.txt", "world", "kirakira"))
            self.assertEqual((workdir / "a.txt").read_text(), "hello\nkirakira")

    def test_registry_executes_and_handles_unknown_tool(self):
        registry = ToolRegistry()
        registry.register(
            ToolSpec("echo", "Echo text", {"type": "object", "properties": {}, "required": []}),
            lambda text: text,
        )
        ok = registry.execute(ToolCall("1", "echo", {"text": "hi"}))
        missing = registry.execute(ToolCall("2", "missing", {}))

        self.assertEqual(ok.content, "hi")
        self.assertTrue(missing.is_error)
        self.assertIn("Unknown tool", missing.content)

    def test_registry_sync_execute_runs_async_tool_when_no_loop(self):
        async def echo_async(text):
            return "async:%s" % text

        registry = ToolRegistry()
        registry.register(
            ToolSpec("echo_async", "Echo text asynchronously", {"type": "object", "properties": {}, "required": []}),
            echo_async,
        )

        result = registry.execute(ToolCall("1", "echo_async", {"text": "hi"}))

        self.assertFalse(result.is_error)
        self.assertEqual(result.content, "async:hi")

    def test_registry_context_is_isolated_between_async_tasks(self):
        async def scenario():
            registry = ToolRegistry()

            async def read_context(delay):
                await asyncio.sleep(delay)
                return registry.context.get("session_key", "")

            registry.register(
                ToolSpec(
                    "read_context",
                    "Read task-local context",
                    {"type": "object", "properties": {}, "required": []},
                ),
                read_context,
            )

            async def run_one(session_key, delay):
                token = registry.set_context(session_key=session_key)
                try:
                    return await registry.execute_async(
                        ToolCall(session_key, "read_context", {"delay": delay})
                    )
                finally:
                    registry.reset_context(token)

            first, second = await asyncio.gather(
                run_one("session:first", 0.03),
                run_one("session:second", 0.01),
            )
            self.assertEqual(first.content, "session:first")
            self.assertEqual(second.content, "session:second")

        asyncio.run(scenario())

    def test_sync_tool_handler_does_not_block_event_loop(self):
        async def scenario():
            registry = ToolRegistry()

            def slow():
                time.sleep(0.05)
                return "done"

            registry.register(
                ToolSpec("slow", "Slow", {"type": "object", "properties": {}}),
                slow,
            )
            ticked = []

            async def ticker():
                await asyncio.sleep(0.01)
                ticked.append(True)

            result, _ = await asyncio.gather(
                registry.execute_async(ToolCall("1", "slow", {})), ticker()
            )
            self.assertEqual(result.content, "done")
            self.assertEqual(ticked, [True])

        asyncio.run(scenario())

    def test_pre_hook_failure_fails_closed_without_invoking_tool(self):
        class BrokenHook:
            name = "broken"
            event = "pre_tool_use"

            def matches(self, _ctx):
                return True

            async def run(self, _ctx):
                raise RuntimeError("hook broke")

        async def scenario():
            invoked = []
            executor = ToolExecutor([BrokenHook()])
            request = ToolExecutionRequest("s", "c", "1", "demo", {})

            async def invoke(_name, _args):
                invoked.append(True)
                return "done"

            with self.assertLogs("agent.tool_hooks", level="ERROR"):
                result = await executor.execute(request, invoke)
            self.assertEqual(result.status, "error")
            self.assertEqual(invoked, [])

        asyncio.run(scenario())

    def test_registry_marks_error_text_as_failed_result(self):
        registry = ToolRegistry()
        registry.register(
            ToolSpec("fails", "Fail", {"type": "object", "properties": {}}),
            lambda: "Error: expected failure",
        )

        result = registry.execute(ToolCall("1", "fails", {}))

        self.assertTrue(result.is_error)

    def test_registry_validates_required_argument_types_before_handler(self):
        called = []
        registry = ToolRegistry()
        registry.register(
            ToolSpec(
                "typed",
                "Typed tool",
                {
                    "type": "object",
                    "properties": {"count": {"type": "integer"}},
                    "required": ["count"],
                },
            ),
            lambda count: called.append(count) or "ok",
        )

        missing = registry.execute(ToolCall("1", "typed", {}))
        wrong = registry.execute(ToolCall("2", "typed", {"count": "one"}))

        self.assertTrue(missing.is_error)
        self.assertTrue(wrong.is_error)
        self.assertEqual(called, [])

    def test_registry_has_passive_research_tools(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = build_default_registry(Path(tmp))
            names = registry.names()

        self.assertIn("list_dir", names)
        self.assertIn("web_fetch", names)
        self.assertIn("web_search", names)
        self.assertIn("message_push", names)
        self.assertIn("tool_search", names)

    def test_web_fetch_reads_local_http(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return

            def do_GET(self):
                body = b"<html><body><h1>Hello</h1><p>Web Fetch OK</p></body></html>"
                self.send_response(200)
                self.send_header("content-type", "text/html")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                tools = WorkspaceTools(Path(tmp), SkillLoader(Path(tmp) / "skills"))
                old_value = os.environ.get("KIRAKIRA_ALLOW_PRIVATE_WEB_FETCH")
                os.environ["KIRAKIRA_ALLOW_PRIVATE_WEB_FETCH"] = "true"
                try:
                    text = tools.web_fetch("http://127.0.0.1:%d/" % port)
                finally:
                    if old_value is None:
                        os.environ.pop("KIRAKIRA_ALLOW_PRIVATE_WEB_FETCH", None)
                    else:
                        os.environ["KIRAKIRA_ALLOW_PRIVATE_WEB_FETCH"] = old_value
            self.assertIn("Web Fetch OK", text)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

    def test_web_fetch_blocks_local_http_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            tools = WorkspaceTools(Path(tmp), SkillLoader(Path(tmp) / "skills"))
            text = tools.web_fetch("http://127.0.0.1:9/")

        self.assertIn("Refusing to fetch private/local address", text)

    def test_web_fetch_returns_source_metadata_and_decodes_compression(self):
        html_body = (
            '<html><head><title>市场日报</title>'
            '<meta property="article:published_time" content="2026-07-20T09:30:00+08:00">'
            '</head><body><p>指数上涨，来源可核验。</p></body></html>'
        ).encode("utf-8")

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return

            def do_GET(self):
                if self.path == "/gzip":
                    body = gzip.compress(html_body)
                    encoding = "gzip"
                else:
                    body = zlib.compress(html_body)
                    encoding = "deflate"
                self.send_response(200)
                self.send_header("content-type", "text/html; charset=utf-8")
                self.send_header("content-encoding", encoding)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(
                os.environ, {"KIRAKIRA_ALLOW_PRIVATE_WEB_FETCH": "true"}
            ):
                tools = WorkspaceTools(Path(tmp), SkillLoader(Path(tmp) / "skills"))
                for path in ("gzip", "deflate"):
                    with self.subTest(path=path):
                        payload = json.loads(
                            tools.web_fetch("http://127.0.0.1:%d/%s" % (port, path))
                        )
                        self.assertEqual(payload["source"]["title"], "市场日报")
                        self.assertEqual(
                            payload["source"]["published_at"],
                            "2026-07-20T09:30:00+08:00",
                        )
                        self.assertEqual(
                            payload["source"]["url"],
                            "http://127.0.0.1:%d/%s" % (port, path),
                        )
                        self.assertIn("指数上涨", payload["content"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

    def test_web_fetch_honors_declared_charset_and_rejects_garbled_text(self):
        chinese = "中文编码内容".encode("gb18030")

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return

            def do_GET(self):
                if self.path == "/garbled":
                    body = ("\ufffd" * 100).encode("utf-8")
                    content_type = "text/plain; charset=utf-8"
                else:
                    body = chinese
                    content_type = "text/plain; charset=gb18030"
                self.send_response(200)
                self.send_header("content-type", content_type)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(
                os.environ, {"KIRAKIRA_ALLOW_PRIVATE_WEB_FETCH": "true"}
            ):
                tools = WorkspaceTools(Path(tmp), SkillLoader(Path(tmp) / "skills"))
                decoded = json.loads(
                    tools.web_fetch("http://127.0.0.1:%d/encoded" % port)
                )
                self.assertEqual(decoded["content"], "中文编码内容")
                self.assertEqual(decoded["source"]["charset"], "gb18030")
                error = tools.web_fetch("http://127.0.0.1:%d/garbled" % port)
                self.assertTrue(error.startswith("Error:"), error)
                self.assertIn("garbled", error)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

    def test_web_fetch_http_errors_are_tool_errors(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return

            def do_GET(self):
                self.send_error(403 if self.path == "/forbidden" else 404)

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(
                os.environ, {"KIRAKIRA_ALLOW_PRIVATE_WEB_FETCH": "true"}
            ):
                registry = build_default_registry(Path(tmp))
                for path, code in (("forbidden", "403"), ("missing", "404")):
                    with self.subTest(path=path):
                        result = registry.execute(
                            ToolCall(
                                path,
                                "web_fetch",
                                {"url": "http://127.0.0.1:%d/%s" % (port, path)},
                            )
                        )
                        self.assertTrue(result.is_error)
                        self.assertIn(code, result.content)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

    def test_web_search_no_results_and_network_failures_are_tool_errors(self):
        # web_search 走 Exa MCP（参考 akashic-agent）：空结果与网络失败都必须是 tool error。
        empty_sse = _exa_sse({"result": {"content": []}})
        with tempfile.TemporaryDirectory() as tmp:
            registry = build_default_registry(Path(tmp))
            with mock.patch(
                "httpx.Client", return_value=_FakeHttpxClient(text=empty_sse)
            ):
                empty = registry.execute(
                    ToolCall("empty", "web_search", {"query": "missing topic"})
                )
            with mock.patch(
                "httpx.Client",
                return_value=_FakeHttpxClient(exc=__import__("httpx").HTTPError("offline")),
            ):
                failed = registry.execute(
                    ToolCall("failed", "web_search", {"query": "latest news"})
                )

        self.assertTrue(empty.is_error)
        self.assertIn("no results", empty.content)
        self.assertTrue(failed.is_error)
        self.assertIn("Web search failed", failed.content)

    def test_web_search_returns_exa_result_text(self):
        text = (
            "Title: Verified & Dated Report\n"
            "URL: https://example.com/news?id=7\n"
            "Highlights:\n2026 market summary"
        )
        body = _exa_sse({"result": {"content": [{"type": "text", "text": text}]}})

        with tempfile.TemporaryDirectory() as tmp:
            tools = WorkspaceTools(Path(tmp), SkillLoader(Path(tmp) / "skills"))
            with mock.patch("httpx.Client", return_value=_FakeHttpxClient(text=body)):
                payload = json.loads(tools.web_search("market report"))

        self.assertEqual(payload["query"], "market report")
        self.assertIn("https://example.com/news?id=7", payload["result"])
        self.assertIn("Verified & Dated Report", payload["result"])

    def test_message_push_publishes_outbound(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                bus = MessageBus()
                registry = build_default_registry(Path(tmp), bus=bus)
                result = await registry.execute_async(
                    ToolCall(
                        "1",
                        "message_push",
                        {"channel": "cli", "chat_id": "c1", "message": "hello"},
                    )
                )
                envelope = await asyncio.wait_for(bus._outbound.get(), timeout=1)
                outbound = envelope.message
                self.assertEqual(result.content, "已发送")
                self.assertIsInstance(outbound, OutboundMessage)
                self.assertEqual(outbound.content, "hello")

        asyncio.run(scenario())

    def test_background_shell_can_be_polled_and_cleaned(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                registry = build_default_registry(Path(tmp))
                started = await registry.execute_async(
                    ToolCall(
                        "1",
                        "bash",
                        {
                            "command": (
                                shlex.quote(sys.executable)
                                + " -c \"import time; print('start', flush=True); "
                                "time.sleep(0.1); print('done')\""
                            ),
                            "run_in_background": True,
                        },
                    )
                )
                task_id = json.loads(started.content)["background_task_id"]
                output = await registry.execute_async(
                    ToolCall(
                        "2",
                        "task_output",
                        {"task_id": task_id, "block": True, "timeout_ms": 2000},
                    )
                )
                payload = json.loads(output.content)
                self.assertTrue(payload["done"])
                self.assertIn("start", payload["output"])
                self.assertIn("done", payload["output"])
                stopped = await registry.execute_async(
                    ToolCall("3", "task_stop", {"task_id": task_id})
                )
                self.assertEqual(json.loads(stopped.content)["status"], "stopped")
                await registry.shutdown()

        asyncio.run(scenario())

    def test_registry_shutdown_kills_background_shell(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                registry = build_default_registry(Path(tmp))
                started = await registry.execute_async(
                    ToolCall(
                        "1",
                        "bash",
                        {
                            "command": "python -c \"import time; time.sleep(30)\"",
                            "run_in_background": True,
                        },
                    )
                )
                task_id = json.loads(started.content)["background_task_id"]
                await asyncio.wait_for(registry.shutdown(), timeout=2.0)
                result = await registry.execute_async(
                    ToolCall("2", "task_output", {"task_id": task_id})
                )
                self.assertTrue(result.is_error)

        asyncio.run(scenario())

    def test_shell_execution_is_isolated_by_session_owner(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                registry = build_default_registry(Path(tmp))
                owner_a = registry.set_context(session_key="web:a")
                try:
                    started = await registry.execute_async(
                        ToolCall(
                            "1",
                            "bash",
                            {
                                "command": "python -c \"import time; time.sleep(30)\"",
                                "run_in_background": True,
                            },
                        )
                    )
                    execution_id = json.loads(started.content)["execution_id"]
                finally:
                    registry.reset_context(owner_a)

                owner_b = registry.set_context(session_key="web:b")
                try:
                    denied = await registry.execute_async(
                        ToolCall(
                            "2",
                            "task_output",
                            {"task_id": execution_id},
                        )
                    )
                    self.assertTrue(denied.is_error)
                finally:
                    registry.reset_context(owner_b)

                await registry.cleanup_owner("web:a")
                self.assertFalse(
                    registry._tools["bash"].handler.__self__._shell_processes._executions
                )
                await registry.shutdown()

        asyncio.run(scenario())

    @unittest.skipIf(os.name == "nt", "PTY test requires POSIX")
    def test_tty_shell_accepts_write_stdin(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                registry = build_default_registry(Path(tmp))
                token = registry.set_context(session_key="web:tty")
                try:
                    started = await registry.execute_async(
                        ToolCall(
                            "1",
                            "bash",
                            {
                                "command": "read value; echo got:$value",
                                "tty": True,
                                "run_in_background": True,
                            },
                        )
                    )
                    execution_id = json.loads(started.content)["execution_id"]
                    resumed = await registry.execute_async(
                        ToolCall(
                            "2",
                            "write_stdin",
                            {
                                "execution_id": execution_id,
                                "chars": "hello\n",
                                "yield_time_ms": 2000,
                            },
                        )
                    )
                    payload = json.loads(resumed.content)
                    self.assertEqual(payload["process_status"], "succeeded")
                    self.assertIn("got:hello", payload["output"])
                finally:
                    registry.reset_context(token)
                    await registry.shutdown()

        asyncio.run(scenario())

    def test_tool_search_returns_matching_tool(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = build_default_registry(Path(tmp))
            result = registry.execute(ToolCall("1", "tool_search", {"query": "fetch"}))
            payload = json.loads(result.content)

        self.assertTrue(any(item["name"] == "web_fetch" for item in payload["matched"]))
        self.assertTrue(all("risk" in item for item in payload["matched"]))

    def test_tool_meta_tracks_risk_source_and_deferred_catalog(self):
        registry = ToolRegistry()
        registry.register(
            ToolSpec("remote_write", "write remotely", {"type": "object"}),
            lambda: "ok",
            deferred=True,
            risk="external-side-effect",
            search_hint="remote mutation",
            source_type="plugin",
            source_name="demo",
        )

        meta = registry.get_meta("remote_write")
        self.assertEqual(meta.risk, "external-side-effect")
        self.assertEqual(meta.source_type, "plugin")
        self.assertEqual(meta.source_name, "demo")
        self.assertEqual(registry.get_deferred_names()["plugin"], ["remote_write"])


if __name__ == "__main__":
    unittest.main()
