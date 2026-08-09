"""Stdlib HTTP web channel.

This is a dependency-light passive web channel. It exposes a tiny chat page and
JSON endpoints, then routes each message through the same MessageBus as every
other channel.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
import urllib.parse
from uuid import uuid4

from infra.channels.contract import ChannelContext
from frontend.web_ui import CHAT_HTML, DASHBOARD_HTML
from bus.events import (
    ChannelMessage,
    DeliveryReceipt,
    DeliveryStatus,
    InboundMessage,
    OutboundMessage,
)

logger = logging.getLogger(__name__)


class _DaemonThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True


class WebChannel:
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 6322,
        channel_name: str = "web",
        response_timeout: float = 180.0,
        dashboard: Any = None,
    ) -> None:
        self.name = channel_name
        self.host = host
        self.port = int(port)
        self.response_timeout = float(response_timeout)
        # 装配期注入的 DashboardService;未注入时 _dashboard() 按 ctx 临时组一个。
        self._dashboard_service = dashboard
        self._ctx: ChannelContext | None = None
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending: dict[str, asyncio.Future[OutboundMessage]] = {}
        self._event_queues: dict[str, asyncio.Queue[OutboundMessage]] = {}
        self._lock = threading.Lock()
        self._push_registration = None

    async def start(self, ctx: ChannelContext) -> None:
        self._ctx = ctx
        self._loop = asyncio.get_running_loop()
        if ctx.push_tool is not None:
            self._push_registration = ctx.push_tool.register_channel(
                self.name, self._deliver_message
            )
        ctx.bus.subscribe_outbound(self.name, self._on_response)
        handler = self._handler_factory()
        self._server = _DaemonThreadingHTTPServer((self.host, self.port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, name="kirakira-web", daemon=True)
        self._thread.start()
        ctx.log.info("web channel listening on http://%s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=3)
        self._server = None
        self._thread = None
        if self._ctx is not None:
            self._ctx.bus.unsubscribe_outbound(self.name, self._on_response)
        if self._push_registration is not None:
            self._push_registration.close()
            self._push_registration = None
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for future in pending:
            if not future.done():
                future.cancel()
        self._ctx = None
        self._loop = None

    async def _on_response(self, msg: OutboundMessage) -> None:
        request_id = str(msg.metadata.get("client_request_id") or "")
        future = None
        with self._lock:
            if request_id:
                future = self._pending.pop(request_id, None)
        if future is not None and not future.done():
            future.set_result(msg)
            return
        if not request_id:
            queue = self._event_queues.setdefault(msg.chat_id, asyncio.Queue())
            await queue.put(msg)

    async def _deliver_message(self, message: ChannelMessage) -> DeliveryReceipt:
        """把完整逻辑消息作为一个 Web event 提交。"""

        media = [attachment.source for attachment in message.attachments]
        await self._on_response(
            OutboundMessage(
                channel=message.channel,
                chat_id=message.chat_id,
                content=message.content,
                thinking=message.thinking or "",
                media=media,
                metadata=dict(message.metadata),
                session_message_id=message.session_message_id,
                control_turn_id=message.control_turn_id,
            )
        )
        return DeliveryReceipt(
            DeliveryStatus.SUCCESS,
            canonical_media=tuple(media),
        )

    def _handler_factory(self):
        channel = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                logger.info("[web] " + fmt, *args)

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                if parsed.path == "/":
                    self._send_bytes(CHAT_HTML.encode("utf-8"), "text/html; charset=utf-8")
                    return
                if parsed.path in ("/dashboard", "/memory"):
                    # /memory 是旧地址,保留为仪表盘的别名,老书签不至于 404。
                    self._send_bytes(DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")
                    return
                if parsed.path == "/favicon.ico":
                    self._send_bytes(b"", "image/x-icon")
                    return
                if parsed.path == "/health":
                    self._send_json({"ok": True, "channel": channel.name})
                    return
                if parsed.path.startswith("/api/dashboard"):
                    self._dashboard_get(parsed)
                    return
                if parsed.path == "/api/sessions":
                    self._send_json({"sessions": channel._dashboard().sessions()})
                    return
                if parsed.path == "/api/memories":
                    query = urllib.parse.parse_qs(parsed.query)
                    self._send_json(channel._dashboard().memories(query))
                    return
                if parsed.path == "/api/memory":
                    query = urllib.parse.parse_qs(parsed.query)
                    item = channel._dashboard().memory_item(str((query.get("id") or [""])[0]))
                    self._send_json(
                        {"memory": item},
                        status=HTTPStatus.OK if item is not None else HTTPStatus.NOT_FOUND,
                    )
                    return
                if parsed.path == "/api/memory/similar":
                    query = urllib.parse.parse_qs(parsed.query)
                    try:
                        items = channel._dashboard().memory_similar(
                            str((query.get("id") or [""])[0]),
                            int((query.get("limit") or ["8"])[0]),
                        )
                        self._send_json({"items": items})
                    except Exception as exc:
                        self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                if parsed.path == "/api/memory/health":
                    self._send_json(channel._dashboard().memory_health())
                    return
                if parsed.path == "/events":
                    try:
                        query = urllib.parse.parse_qs(parsed.query)
                        session_id = str((query.get("session_id") or [""])[0])
                        if not session_id:
                            raise ValueError("session_id is required")
                        message = channel._next_event_sync(channel._chat_id(session_id))
                        self._send_json(
                            {
                                "content": message.content,
                                "media": message.media,
                                "metadata": message.metadata,
                            }
                        )
                    except TimeoutError:
                        self._send_json({"content": ""})
                    except Exception as exc:
                        self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:
                if self.path in ("/api/memories/delete", "/api/dashboard/memories/batch-delete"):
                    try:
                        payload = self._read_json()
                        deleted = channel._dashboard().delete_memories(
                            [str(item) for item in payload.get("ids", [])],
                            confirm=str(payload.get("confirm") or ""),
                        )
                        self._send_json({"ok": True, "deleted": deleted})
                    except Exception as exc:
                        self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                if self.path == "/interrupt":
                    try:
                        payload = self._read_json()
                        session_id = str(payload.get("session_id") or payload.get("chat_id") or "")
                        if not session_id or channel._ctx is None or channel._ctx.interrupt is None:
                            raise ValueError("session_id is required and interrupt must be enabled")
                        stopped = channel._ctx.interrupt(
                            "%s:%s" % (channel.name, channel._chat_id(session_id))
                        )
                        self._send_json({"ok": True, "interrupted": stopped})
                    except Exception as exc:
                        self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                if self.path != "/message":
                    self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
                    return
                try:
                    payload = self._read_json()
                    result = channel._handle_message_sync(payload)
                    self._send_json(result)
                except Exception as exc:
                    logger.exception("[web] request failed")
                    self._send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

            def do_PATCH(self) -> None:
                if self.path not in ("/api/memory", "/api/dashboard/memory"):
                    self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
                    return
                try:
                    payload = self._read_json()
                    updated = channel._dashboard().update_memory(payload)
                    self._send_json({"ok": updated}, status=HTTPStatus.OK if updated else HTTPStatus.NOT_FOUND)
                except Exception as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

            def do_DELETE(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                query = urllib.parse.parse_qs(parsed.query)
                board = channel._dashboard()
                if parsed.path in ("/api/session", "/api/dashboard/session"):
                    key = str((query.get("key") or [""])[0])
                    deleted = board.delete_session(key)
                    self._send_json({"ok": deleted}, status=HTTPStatus.OK if deleted else HTTPStatus.NOT_FOUND)
                    return
                if parsed.path in ("/api/memory", "/api/dashboard/memory"):
                    memory_id = str((query.get("id") or [""])[0])
                    hard = str((query.get("hard") or [""])[0]).lower() in {"1", "true"}
                    if hard:
                        try:
                            deleted = bool(
                                board.delete_memories(
                                    [memory_id],
                                    confirm=str((query.get("confirm") or [""])[0]),
                                )
                            )
                        except Exception as exc:
                            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                            return
                    else:
                        deleted = board.forget_memory(memory_id)
                    self._send_json({"ok": deleted}, status=HTTPStatus.OK if deleted else HTTPStatus.NOT_FOUND)
                    return
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

            def _dashboard_get(self, parsed) -> None:
                """`/api/dashboard/*` 的只读投影。单个面板失败不让整页 500。"""
                query = urllib.parse.parse_qs(parsed.query)
                board = channel._dashboard()
                suffix = parsed.path[len("/api/dashboard") :]
                try:
                    if suffix == "/overview":
                        self._send_json(board.overview())
                    elif suffix == "/memories":
                        self._send_json(board.memories(query))
                    elif suffix == "/memory":
                        item = board.memory_item(str((query.get("id") or [""])[0]))
                        self._send_json(
                            {"memory": item},
                            status=HTTPStatus.OK if item is not None else HTTPStatus.NOT_FOUND,
                        )
                    elif suffix == "/memory/similar":
                        self._send_json(
                            {
                                "items": board.memory_similar(
                                    str((query.get("id") or [""])[0]),
                                    int((query.get("limit") or ["8"])[0]),
                                )
                            }
                        )
                    elif suffix == "/memory/engine-info":
                        self._send_json(board.engine_info())
                    elif suffix == "/memory/health":
                        self._send_json(board.memory_health())
                    elif suffix == "/sessions":
                        self._send_json({"sessions": board.sessions()})
                    elif suffix == "/session":
                        item = board.session(str((query.get("key") or [""])[0]))
                        self._send_json(
                            {"session": item},
                            status=HTTPStatus.OK if item is not None else HTTPStatus.NOT_FOUND,
                        )
                    elif suffix == "/messages":
                        self._send_json(board.messages(query))
                    elif suffix == "/recall":
                        self._send_json(board.recall_turns(query))
                    elif suffix == "/recall/turn":
                        item = board.recall_turn(str((query.get("id") or [""])[0]))
                        self._send_json(
                            {"turn": item},
                            status=HTTPStatus.OK if item is not None else HTTPStatus.NOT_FOUND,
                        )
                    elif suffix == "/plugins":
                        self._send_json(board.plugins())
                    elif suffix == "/proactive":
                        self._send_json(board.proactive())
                    elif suffix == "/drift":
                        self._send_json(board.drift())
                    else:
                        self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
                except Exception as exc:  # noqa: BLE001 - 面板错误回给前端展示,不 500
                    logger.exception("[web] dashboard %s failed", suffix)
                    self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

            def _read_json(self) -> dict[str, Any]:
                length = int(self.headers.get("content-length") or "0")
                if length < 0 or length > 1024 * 1024:
                    raise ValueError("request body exceeds 1 MB")
                raw = self.rfile.read(length).decode("utf-8")
                data = json.loads(raw or "{}")
                if not isinstance(data, dict):
                    raise ValueError("JSON body must be an object")
                return data

            def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(int(status))
                self.send_header("content-type", "application/json; charset=utf-8")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _send_bytes(self, body: bytes, content_type: str) -> None:
                self.send_response(HTTPStatus.OK)
                self.send_header("content-type", content_type)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler

    def _handle_message_sync(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._ctx is None or self._loop is None:
            raise RuntimeError("web channel is not started")
        text = str(payload.get("text") or payload.get("content") or "").strip()
        if not text:
            raise ValueError("text/content is required")
        session_id = str(payload.get("session_id") or payload.get("chat_id") or uuid4().hex).strip()
        if not session_id or len(session_id) > 200:
            raise ValueError("session_id must contain 1-200 characters")
        chat_id = self._chat_id(session_id)
        if text.lower() == "/stop":
            interrupted = bool(
                self._ctx.interrupt
                and self._ctx.interrupt("%s:%s" % (self.name, chat_id))
            )
            return {
                "channel": self.name,
                "chat_id": chat_id,
                "session_id": "%s:%s" % (self.name, chat_id),
                "content": "本轮已中断。" if interrupted else "当前没有正在执行的任务。",
                "thinking": "",
                "media": [],
                "metadata": {"interrupted": interrupted},
            }
        request_id = str(payload.get("request_id") or uuid4().hex).strip()
        payload = {**payload, "request_id": request_id}
        future = asyncio.run_coroutine_threadsafe(
            self._publish_and_wait(chat_id=chat_id, text=text, payload=payload),
            self._loop,
        )
        msg = future.result(timeout=self.response_timeout + 5)
        return {
            "channel": msg.channel,
            "chat_id": msg.chat_id,
            "session_id": "%s:%s" % (self.name, msg.chat_id),
            "content": msg.content,
            "thinking": msg.thinking,
            "media": msg.media,
            "metadata": msg.metadata,
        }

    async def _publish_and_wait(self, *, chat_id: str, text: str, payload: dict[str, Any]) -> OutboundMessage:
        if self._ctx is None:
            raise RuntimeError("web channel is not started")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[OutboundMessage] = loop.create_future()
        request_id = str(payload["request_id"])
        with self._lock:
            if request_id in self._pending:
                raise ValueError("duplicate request_id: %s" % request_id)
            self._pending[request_id] = future
        try:
            media = self._validated_media(payload.get("media"))
            await self._ctx.bus.publish_inbound(
                InboundMessage(
                    channel=self.name,
                    sender=str(payload.get("sender") or "web"),
                    chat_id=chat_id,
                    content=text,
                    media=media,
                    metadata={"client_request_id": request_id},
                )
            )
            return await asyncio.wait_for(future, timeout=self.response_timeout)
        finally:
            with self._lock:
                if self._pending.get(request_id) is future:
                    self._pending.pop(request_id, None)

    def _chat_id(self, session_id: str) -> str:
        prefix = "%s:" % self.name
        if session_id.startswith(prefix):
            return session_id[len(prefix):]
        return session_id

    def _next_event_sync(self, chat_id: str) -> OutboundMessage:
        if self._loop is None:
            raise RuntimeError("web channel is not started")

        async def wait() -> OutboundMessage:
            queue = self._event_queues.setdefault(chat_id, asyncio.Queue())
            return await asyncio.wait_for(queue.get(), timeout=25.0)

        future = asyncio.run_coroutine_threadsafe(wait(), self._loop)
        try:
            return future.result(timeout=30.0)
        except Exception:
            future.cancel()
            raise TimeoutError

    def _validated_media(self, value: object) -> list[str]:
        if self._ctx is None or not isinstance(value, list):
            return []
        root = self._ctx.workspace.resolve()
        result = []
        for item in value[:8]:
            path = Path(str(item)).expanduser()
            if not path.is_absolute():
                path = root / path
            path = path.resolve()
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise ValueError("media path escapes workspace") from exc
            if not path.is_file():
                raise ValueError("media file does not exist: %s" % item)
            result.append(str(path))
        return result

    def _dashboard(self):
        """Dashboard 数据面。

        优先用装配期注入的 DashboardService(它能看到 proactive/drift/plugins);
        没注入时(最小构造、旧测试)按 ctx 现有依赖临时组一个,页面仍可打开,
        只是主动/插件面板为空。
        """
        if self._dashboard_service is not None:
            return self._dashboard_service
        from bootstrap.dashboard_api import DashboardService

        ctx = self._ctx
        return DashboardService(
            workspace=ctx.workspace if ctx else Path("."),
            session_manager=ctx.session_manager if ctx else None,
            memory=getattr(ctx, "memory", None) if ctx else None,
            memory_services=getattr(ctx, "memory_services", None) if ctx else None,
        )
