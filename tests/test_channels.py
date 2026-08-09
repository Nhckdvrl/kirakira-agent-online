"""Channel integration tests."""

import asyncio
import json
import socket
import tempfile
import threading
import unittest
import urllib.request
from types import SimpleNamespace
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from bus.queue import MessageBus
from infra.channels.contract import ChannelContext
from infra.channels.qq_channel import QQChannel
from infra.channels.qqbot_channel import QQBotChannel
from infra.channels.telegram_channel import TelegramChannel
from infra.channels.web_chat_channel import WebChannel
from agent.prompting.context_builder import ContextBuilder
from bus.event_bus import EventBus
from core.memory.legacy import MemoryRuntime
from agent.core.runtime import AgentLoop, DefaultReasoner, PassiveTurnPipeline, RuntimeConfig
from core.schema import ModelResponse
from bus.events import OutboundMessage
from session.manager import SessionManager
from agent.tools import build_default_registry


class FakeModel:
    def __init__(self, text):
        self.text = text

    def complete(self, messages, tools, system, model, max_tokens):
        return ModelResponse(text=self.text)


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def build_runtime(workdir, text):
    bus = MessageBus()
    event_bus = EventBus()
    sessions = SessionManager(workdir)
    memory = MemoryRuntime(workdir, session_manager=sessions)
    tools = build_default_registry(workdir, memory=memory, session_manager=sessions)
    context = ContextBuilder(workdir, memory)
    config = RuntimeConfig(model="fake", max_iterations=3, max_tokens=1000, history_window=20)
    reasoner = DefaultReasoner(
        model_client=FakeModel(text),
        tools=tools,
        config=config,
        context=context,
        event_bus=event_bus,
    )
    pipeline = PassiveTurnPipeline(
        bus=bus,
        event_bus=event_bus,
        session_manager=sessions,
        memory=memory,
        tools=tools,
        reasoner=reasoner,
        config=config,
    )
    return bus, event_bus, sessions, AgentLoop(bus=bus, pipeline=pipeline)


async def start_core(bus, loop):
    return [
        asyncio.create_task(loop.run()),
        asyncio.create_task(bus.dispatch_outbound()),
    ]


async def stop_core(bus, loop, tasks):
    loop.stop()
    bus.stop()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


class FakeOneBotServer:
    def __init__(self, body=b'{"status":"ok","retcode":0}'):
        self.port = free_port()
        self.received = []
        self.body = body
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return

            def do_POST(self):
                length = int(self.headers.get("content-length") or "0")
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                outer.received.append((self.path, payload))
                body = outer.body
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


class ChannelTests(unittest.TestCase):
    def test_official_qqbot_c2c_inbound_and_outbound_contract(self):
        class FakeResponse:
            status_code = 200

            def raise_for_status(self):
                return None

        class FakeClient:
            def __init__(self):
                self.posts = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def post(self, url, **kwargs):
                self.posts.append((url, kwargs))
                return FakeResponse()

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                bus = MessageBus()
                sessions = SessionManager(root)
                channel = QQBotChannel(
                    app_id="app",
                    client_secret="secret",
                    allow_from=["openid-1"],
                )
                channel._ctx = ChannelContext(
                    bus=bus,
                    session_manager=sessions,
                    event_bus=EventBus(),
                    workspace=root,
                    log=__import__("logging").getLogger("test.qqbot"),
                )
                channel._token = "token"
                channel._token_expires_at = float("inf")
                await channel._handle_c2c(
                    {
                        "id": "message-1",
                        "content": "你好",
                        "author": {"user_openid": "openid-1"},
                    }
                )
                inbound = await bus.consume_inbound()
                self.assertEqual(inbound.channel, "qqbot")
                self.assertEqual(inbound.chat_id, "c2c:openid-1")
                self.assertEqual(inbound.content, "你好")
                await bus.complete_inbound(inbound)

                client = FakeClient()
                with mock.patch(
                    "infra.channels.qqbot_channel.httpx.AsyncClient",
                    return_value=client,
                ):
                    await channel._on_response(
                        OutboundMessage("qqbot", "c2c:openid-1", "收到")
                    )
                self.assertEqual(
                    client.posts[0][0],
                    "https://api.sgroup.qq.com/v2/users/openid-1/messages",
                )
                self.assertEqual(client.posts[0][1]["json"]["msg_id"], "message-1")

        asyncio.run(scenario())

    def test_web_channel_correlates_concurrent_requests_in_same_session(self):
        class EchoModel:
            def complete(self, messages, tools, system, model, max_tokens):
                current = str(messages[-1].get("content") or "").splitlines()[-1]
                return ModelResponse(text="reply:" + current)

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                bus = MessageBus()
                event_bus = EventBus()
                sessions = SessionManager(workdir)
                memory = MemoryRuntime(workdir, session_manager=sessions)
                tools = build_default_registry(workdir, memory=memory, session_manager=sessions)
                config = RuntimeConfig(model="fake", max_iterations=3)
                reasoner = DefaultReasoner(
                    model_client=EchoModel(),
                    tools=tools,
                    config=config,
                    context=ContextBuilder(workdir, memory),
                    event_bus=event_bus,
                )
                pipeline = PassiveTurnPipeline(
                    bus=bus,
                    event_bus=event_bus,
                    session_manager=sessions,
                    memory=memory,
                    tools=tools,
                    reasoner=reasoner,
                    config=config,
                )
                loop = AgentLoop(bus=bus, pipeline=pipeline)
                port = free_port()
                channel = WebChannel(host="127.0.0.1", port=port)
                ctx = ChannelContext(
                    bus,
                    sessions,
                    event_bus,
                    workdir,
                    __import__("logging").getLogger("test.web.concurrent"),
                )
                tasks = await start_core(bus, loop)
                await channel.start(ctx)

                def post(text, request_id):
                    request = urllib.request.Request(
                        "http://127.0.0.1:%d/message" % port,
                        data=json.dumps(
                            {
                                "session_id": "same",
                                "request_id": request_id,
                                "text": text,
                            }
                        ).encode("utf-8"),
                        headers={"content-type": "application/json"},
                        method="POST",
                    )
                    return json.loads(
                        urllib.request.urlopen(request, timeout=10).read().decode("utf-8")
                    )

                try:
                    first, second = await asyncio.gather(
                        asyncio.to_thread(post, "first", "r1"),
                        asyncio.to_thread(post, "second", "r2"),
                    )
                    self.assertEqual(
                        {first["content"], second["content"]},
                        {"reply:first", "reply:second"},
                    )
                finally:
                    await channel.stop()
                    await stop_core(bus, loop, tasks)

        asyncio.run(scenario())

    def test_web_channel_posts_message_and_returns_agent_reply(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                bus, event_bus, sessions, loop = build_runtime(workdir, "web ok")
                port = free_port()
                channel = WebChannel(host="127.0.0.1", port=port)
                ctx = ChannelContext(bus, sessions, event_bus, workdir, __import__("logging").getLogger("test.web"))
                tasks = await start_core(bus, loop)
                await channel.start(ctx)
                try:
                    data = json.dumps({"session_id": "test-web", "text": "hello"}).encode("utf-8")
                    req = urllib.request.Request(
                        "http://127.0.0.1:%d/message" % port,
                        data=data,
                        headers={"content-type": "application/json"},
                        method="POST",
                    )
                    body = await asyncio.to_thread(lambda: urllib.request.urlopen(req, timeout=10).read())
                    payload = json.loads(body.decode("utf-8"))
                    self.assertEqual(payload["content"], "web ok")
                    self.assertEqual(sessions.get_or_create("web:test-web").messages[-1]["content"], "web ok")
                finally:
                    await channel.stop()
                    await stop_core(bus, loop, tasks)

        asyncio.run(scenario())

    def test_web_channel_long_poll_receives_unsolicited_outbound(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                bus = MessageBus()
                event_bus = EventBus()
                sessions = SessionManager(workdir)
                port = free_port()
                channel = WebChannel(host="127.0.0.1", port=port)
                ctx = ChannelContext(
                    bus,
                    sessions,
                    event_bus,
                    workdir,
                    __import__("logging").getLogger("test.web.events"),
                )
                dispatcher = asyncio.create_task(bus.dispatch_outbound())
                await channel.start(ctx)

                def poll():
                    return json.loads(
                        urllib.request.urlopen(
                            "http://127.0.0.1:%d/events?session_id=events" % port,
                            timeout=10,
                        )
                        .read()
                        .decode("utf-8")
                    )

                try:
                    poll_task = asyncio.create_task(asyncio.to_thread(poll))
                    await asyncio.sleep(0.05)
                    await bus.publish_outbound(
                        OutboundMessage("web", "events", "scheduled hello")
                    )
                    payload = await asyncio.wait_for(poll_task, timeout=3)
                    self.assertEqual(payload["content"], "scheduled hello")
                finally:
                    bus.stop()
                    await channel.stop()
                    await dispatcher

        asyncio.run(scenario())

    def test_web_management_api_lists_updates_and_forgets_memory(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                bus = MessageBus()
                event_bus = EventBus()
                sessions = SessionManager(workdir)
                memory = MemoryRuntime(workdir, session_manager=sessions)
                record = memory.memorize("original memory")
                port = free_port()
                channel = WebChannel(host="127.0.0.1", port=port)
                ctx = ChannelContext(
                    bus,
                    sessions,
                    event_bus,
                    workdir,
                    __import__("logging").getLogger("test.web.management"),
                    memory=memory,
                )
                await channel.start(ctx)
                try:
                    dashboard = (
                        await asyncio.to_thread(
                            lambda: urllib.request.urlopen(
                                "http://127.0.0.1:%d/memory" % port,
                                timeout=5,
                            ).read().decode("utf-8")
                        )
                    )
                    # /memory 现在是仪表盘的旧地址别名(老书签不至于 404)
                    self.assertIn("Kirakira 仪表盘", dashboard)
                    self.assertIn('data-tab="memory"', dashboard)
                    listed = json.loads(
                        await asyncio.to_thread(
                            lambda: urllib.request.urlopen(
                                "http://127.0.0.1:%d/api/memories" % port,
                                timeout=5,
                            ).read()
                        )
                    )
                    self.assertEqual(listed["memories"][0]["id"], record.id)

                    patch_request = urllib.request.Request(
                        "http://127.0.0.1:%d/api/memory" % port,
                        data=json.dumps(
                            {"id": record.id, "content": "updated memory"}
                        ).encode("utf-8"),
                        headers={"content-type": "application/json"},
                        method="PATCH",
                    )
                    await asyncio.to_thread(
                        lambda: urllib.request.urlopen(patch_request, timeout=5).read()
                    )
                    self.assertEqual(memory.recall("updated")[0].content, "updated memory")

                    delete_request = urllib.request.Request(
                        "http://127.0.0.1:%d/api/memory?id=%s" % (port, record.id),
                        method="DELETE",
                    )
                    await asyncio.to_thread(
                        lambda: urllib.request.urlopen(delete_request, timeout=5).read()
                    )
                    self.assertEqual(memory.recall("updated"), [])
                finally:
                    await channel.stop()

        asyncio.run(scenario())

    def test_qq_channel_webhook_routes_group_message_and_sends_reply(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                onebot = FakeOneBotServer()
                onebot.start()
                bus, event_bus, sessions, loop = build_runtime(workdir, "qq ok")
                port = free_port()
                channel = QQChannel(
                    bot_uin="12345",
                    api_base_url="http://127.0.0.1:%d" % onebot.port,
                    webhook_host="127.0.0.1",
                    webhook_port=port,
                    group_allow=["777"],
                    require_at=True,
                )
                ctx = ChannelContext(bus, sessions, event_bus, workdir, __import__("logging").getLogger("test.qq"))
                tasks = await start_core(bus, loop)
                await channel.start(ctx)
                try:
                    event = {
                        "post_type": "message",
                        "message_type": "group",
                        "group_id": 777,
                        "user_id": 888,
                        "message_id": 1,
                        "raw_message": "[CQ:at,qq=12345] 你好",
                    }
                    req = urllib.request.Request(
                        "http://127.0.0.1:%d/qq/webhook" % port,
                        data=json.dumps(event).encode("utf-8"),
                        headers={"content-type": "application/json"},
                        method="POST",
                    )
                    body = await asyncio.to_thread(lambda: urllib.request.urlopen(req, timeout=10).read())
                    payload = json.loads(body.decode("utf-8"))
                    self.assertTrue(payload["ok"])
                    await asyncio.sleep(0.5)
                    self.assertEqual(sessions.get_or_create("qq:gqq:777").messages[-1]["content"], "qq ok")
                    self.assertTrue(any(path == "/send_group_msg" for path, _ in onebot.received))
                finally:
                    await channel.stop()
                    await stop_core(bus, loop, tasks)
                    onebot.stop()

        asyncio.run(scenario())

    def test_telegram_allow_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            sessions = SessionManager(Path(tmp))
            channel = TelegramChannel(
                token="test-token",
                bus=MessageBus(),
                session_manager=sessions,
                allow_from=["123", "alice"],
            )

            self.assertTrue(channel._is_allowed(SimpleNamespace(id=123, username=None)))
            self.assertTrue(channel._is_allowed(SimpleNamespace(id=999, username="Alice")))
            self.assertFalse(channel._is_allowed(SimpleNamespace(id=999, username="bob")))
            sessions.close()

    def test_telegram_plain_update_does_not_require_reply_context(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                bus = MessageBus()
                sessions = SessionManager(root)
                channel = TelegramChannel(
                    token="test-token",
                    bus=bus,
                    session_manager=sessions,
                    allow_from=["123"],
                )
                channel._safe_send_typing = mock.AsyncMock()
                message = SimpleNamespace(
                    message_id=2,
                    text="hello",
                    reply_to_message=None,
                )
                update = SimpleNamespace(
                    effective_message=message,
                    effective_chat=SimpleNamespace(id=123),
                    effective_user=SimpleNamespace(id=123, username="alice"),
                )
                await channel._on_message(update, SimpleNamespace(bot=object()))
                inbound = await bus.consume_inbound()
                self.assertEqual(inbound.content, "hello")
                self.assertEqual(inbound.chat_id, "123")
                self.assertEqual(inbound.metadata["username"], "alice")
                await bus.complete_inbound(inbound)
                sessions.close()

        asyncio.run(scenario())

    def test_telegram_outbound_uses_reference_markdown_sender(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                sessions = SessionManager(root)
                channel = TelegramChannel(
                    token="test-token",
                    bus=MessageBus(),
                    session_manager=sessions,
                )
                with mock.patch(
                    "infra.channels.telegram_channel.send_markdown",
                    new=mock.AsyncMock(),
                ) as sender:
                    await channel._on_response(
                        OutboundMessage(
                            "telegram",
                            "1",
                            "### 标题\n\n**重点**",
                        )
                    )
                sender.assert_awaited_once()
                self.assertEqual(sender.await_args.args[1], "1")
                self.assertEqual(sender.await_args.args[2], "### 标题\n\n**重点**")
                sessions.close()

        asyncio.run(scenario())

    def test_qq_api_rejects_failed_retcode(self):
        onebot = FakeOneBotServer(body=b'{"status":"failed","retcode":100,"wording":"bad"}')
        onebot.start()
        try:
            channel = QQChannel(api_base_url="http://127.0.0.1:%d" % onebot.port)
            with self.assertRaises(RuntimeError):
                channel._api("send_private_msg", {"user_id": "1", "message": "hi"})
        finally:
            onebot.stop()


if __name__ == "__main__":
    unittest.main()
