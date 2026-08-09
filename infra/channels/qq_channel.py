"""QQ channel via OneBot/NapCat HTTP webhook and HTTP API."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from infra.channels.base import AttachmentStore, MessageDeduper
from infra.channels.contract import ChannelContext
from bus.events import (
    ChannelMessage,
    DeliveryReceipt,
    InboundMessage,
    OutboundMessage,
    channel_message_from_outbound,
)
from infra.channels.delivery import deliver_message_parts

logger = logging.getLogger(__name__)
_GROUP_PREFIX = "gqq:"
_CQ_AT_RE = re.compile(r"\[CQ:at,qq=(\d+)[^\]]*\]")
_CQ_IMAGE_RE = re.compile(r"\[CQ:image,[^\]]*?url=([^,\]]+)[^\]]*\]")
_CQ_TAG_RE = re.compile(r"\[CQ:[^\]]+\]")


class _DaemonThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True


class QQChannel:
    def __init__(
        self,
        *,
        bot_uin: str = "",
        api_base_url: str = "http://127.0.0.1:3000",
        webhook_host: str = "127.0.0.1",
        webhook_port: int = 8766,
        access_token: str = "",
        allow_from: list[str] | None = None,
        group_allow: list[str] | None = None,
        group_policies: dict[str, dict[str, Any]] | None = None,
        require_at: bool = True,
        channel_name: str = "qq",
    ) -> None:
        self.name = channel_name
        self.bot_uin = str(bot_uin or "")
        self.api_base_url = api_base_url.rstrip("/")
        self.webhook_host = webhook_host
        self.webhook_port = int(webhook_port)
        self.access_token = access_token
        self.allow_from = {str(item) for item in (allow_from or []) if str(item).strip()}
        self.group_allow = {str(item) for item in (group_allow or []) if str(item).strip()}
        self.group_policies = {
            str(key): dict(value) for key, value in (group_policies or {}).items()
        }
        self.require_at = bool(require_at)
        self._ctx: ChannelContext | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._deduper = MessageDeduper()
        self._attachments: AttachmentStore | None = None
        self._push_registration = None

    async def start(self, ctx: ChannelContext) -> None:
        self._ctx = ctx
        self._attachments = AttachmentStore(ctx.workspace / "uploads" / self.name)
        self._loop = asyncio.get_running_loop()
        # Reference 的 NcatBot 启动会真实连接 NapCat；Webhook 替代实现也必须在
        # readiness 前验证 OneBot API，而不是只证明本地端口监听成功。
        await asyncio.to_thread(self._api, "get_status", {})
        if ctx.push_tool is not None:
            self._push_registration = ctx.push_tool.register_channel(
                self.name, self._deliver_message
            )
        ctx.bus.subscribe_outbound(self.name, self._on_response)
        handler = self._handler_factory()
        self._server = _DaemonThreadingHTTPServer(
            (self.webhook_host, self.webhook_port), handler
        )
        self._thread = threading.Thread(target=self._server.serve_forever, name="kirakira-qq", daemon=True)
        self._thread.start()
        ctx.log.info("qq channel webhook listening on http://%s:%s/qq/webhook", self.webhook_host, self.webhook_port)

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
        self._ctx = None
        self._loop = None

    async def _on_response(self, msg: OutboundMessage) -> None:
        receipt = await self._deliver_message(channel_message_from_outbound(msg))
        if not receipt.succeeded:
            raise RuntimeError(receipt.detail or "QQ 消息提交失败")

    async def _deliver_message(self, message: ChannelMessage) -> DeliveryReceipt:
        return await deliver_message_parts(
            message,
            send_text=self._send_text_part,
            send_file=self._send_file_part,
            send_image=self._send_image_part,
        )

    async def _send_text_part(self, chat_id: str, content: str) -> None:
        action, target, target_id = self._outbound_target(chat_id)
        await asyncio.to_thread(
            self._api,
            action,
            {target: target_id, "message": content},
        )

    async def _send_file_part(
        self, chat_id: str, source: str, filename: str | None = None
    ) -> None:
        _ = filename
        await self._send_attachment_part(chat_id, source, kind="file")

    async def _send_image_part(self, chat_id: str, source: str) -> None:
        await self._send_attachment_part(chat_id, source, kind="image")

    async def _send_attachment_part(
        self, chat_id: str, source: str, *, kind: str
    ) -> None:
        path = Path(source).resolve()
        if not path.is_file():
            raise FileNotFoundError(source)
        action, target, target_id = self._outbound_target(chat_id)
        cq = "[CQ:%s,file=file://%s]" % (
            kind,
            urllib.parse.quote(str(path)),
        )
        await asyncio.to_thread(
            self._api,
            action,
            {target: target_id, "message": cq},
        )

    @staticmethod
    def _outbound_target(chat_id: str) -> tuple[str, str, str]:
        if chat_id.startswith(_GROUP_PREFIX):
            return "send_group_msg", "group_id", chat_id[len(_GROUP_PREFIX):]
        return "send_private_msg", "user_id", chat_id

    async def _handle_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        ctx = self._ctx
        if ctx is None:
            return {"ok": False, "error": "channel not started"}
        post_type = str(payload.get("post_type") or "")
        message_type = str(payload.get("message_type") or "")
        if post_type and post_type != "message":
            return {"ok": True, "ignored": "post_type"}
        raw_message, image_urls = self._parse_message(payload)
        user_id = str(payload.get("user_id") or "").strip()
        message_id = str(payload.get("message_id") or "").strip()
        if (not raw_message and not image_urls) or not user_id:
            return {"ok": True, "ignored": "empty"}
        if self.bot_uin and user_id == self.bot_uin:
            return {"ok": True, "ignored": "self_message"}
        group_id = str(payload.get("group_id") or "").strip()
        if self._deduper.seen("%s:%s:%s:%s" % (message_type, group_id, user_id, message_id)):
            return {"ok": True, "ignored": "duplicate"}

        if message_type == "group":
            if not group_id:
                return {"ok": True, "ignored": "missing_group"}
            if self.group_allow and group_id not in self.group_allow:
                return {"ok": True, "ignored": "group_not_allowed"}
            policy = self.group_policies.get(group_id, {})
            policy_allow = {
                str(item) for item in policy.get("allow_from", []) if str(item)
            }
            allowed_users = policy_allow or self.allow_from
            if allowed_users and user_id not in allowed_users:
                return {"ok": True, "ignored": "user_not_allowed"}
            require_at = bool(policy.get("require_at", self.require_at))
            if require_at and self.bot_uin and not self._is_at_bot(raw_message):
                return {"ok": True, "ignored": "not_at_bot"}
            chat_id = _GROUP_PREFIX + group_id
            content = self._strip_at(raw_message)
        else:
            if self.allow_from and user_id not in self.allow_from:
                return {"ok": True, "ignored": "user_not_allowed"}
            chat_id = user_id
            content = _CQ_TAG_RE.sub("", raw_message).strip()

        if content.lower() in ("/stop", "stop"):
            interrupted = bool(
                ctx.interrupt and ctx.interrupt("%s:%s" % (self.name, chat_id))
            )
            message = "本轮已中断。" if interrupted else "当前没有正在执行的任务。"
            action = "send_group_msg" if chat_id.startswith(_GROUP_PREFIX) else "send_private_msg"
            target = "group_id" if action == "send_group_msg" else "user_id"
            target_id = chat_id[len(_GROUP_PREFIX):] if action == "send_group_msg" else chat_id
            await asyncio.to_thread(self._api, action, {target: target_id, "message": message})
            return {"ok": True, "interrupted": interrupted}

        media = await self._download_images(image_urls)
        if not content and not media:
            return {"ok": True, "ignored": "empty_after_filter"}
        if not content:
            content = "[用户发送了图片]"
        if message_type == "group":
            content = "[群聊发送者 QQ:%s]\n%s" % (user_id, content)
        await ctx.bus.publish_inbound(
            InboundMessage(
                channel=self.name,
                sender=user_id,
                chat_id=chat_id,
                content=content,
                media=media,
                metadata={
                    "qq_message_id": message_id,
                    "message_type": message_type,
                    "group_id": str(payload.get("group_id") or ""),
                },
            )
        )
        return {"ok": True, "chat_id": chat_id}

    def _handler_factory(self):
        channel = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                logger.info("[qq] " + fmt, *args)

            def do_GET(self) -> None:
                if self.path == "/health":
                    self._send_json({"ok": True, "channel": channel.name})
                    return
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:
                if self.path not in ("/", "/qq/webhook", "/onebot"):
                    self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
                    return
                try:
                    if channel.access_token:
                        expected = "Bearer %s" % channel.access_token
                        if self.headers.get("Authorization") != expected:
                            self._send_json(
                                {"ok": False, "error": "unauthorized"},
                                status=HTTPStatus.UNAUTHORIZED,
                            )
                            return
                    payload = self._read_json()
                    if channel._loop is None:
                        raise RuntimeError("qq channel loop missing")
                    future = asyncio.run_coroutine_threadsafe(channel._handle_event(payload), channel._loop)
                    self._send_json(future.result(timeout=10))
                except Exception as exc:
                    logger.exception("[qq] webhook failed")
                    self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

            def _read_json(self) -> dict[str, Any]:
                length = int(self.headers.get("content-length") or "0")
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

        return Handler

    def _api(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = "%s/%s" % (self.api_base_url, action)
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = "Bearer %s" % self.access_token
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError("OneBot API %s failed: HTTP %s %s" % (action, exc.code, detail)) from exc
        result = json.loads(body or "{}")
        status = str(result.get("status") or "").lower()
        retcode = result.get("retcode")
        if status and status not in ("ok", "async"):
            raise RuntimeError("OneBot API %s failed: %s" % (action, result))
        if retcode not in (None, 0):
            raise RuntimeError("OneBot API %s failed: %s" % (action, result))
        return result

    def _is_at_bot(self, raw: str) -> bool:
        return any(qq == self.bot_uin for qq in _CQ_AT_RE.findall(raw))

    def _strip_at(self, raw: str) -> str:
        return _CQ_TAG_RE.sub("", _CQ_AT_RE.sub("", raw)).strip()

    def _parse_message(self, payload: dict[str, Any]) -> tuple[str, list[str]]:
        raw = payload.get("message")
        if isinstance(raw, list):
            text_parts = []
            urls = []
            for segment in raw:
                if not isinstance(segment, dict):
                    continue
                kind = str(segment.get("type") or "")
                data = segment.get("data") or {}
                if not isinstance(data, dict):
                    continue
                if kind == "text":
                    text_parts.append(str(data.get("text") or ""))
                elif kind == "at":
                    text_parts.append("[CQ:at,qq=%s]" % str(data.get("qq") or ""))
                elif kind == "image" and data.get("url"):
                    urls.append(str(data["url"]))
            return "".join(text_parts).strip(), urls
        text = str(payload.get("raw_message") or raw or "").strip()
        return text, [urllib.parse.unquote(url) for url in _CQ_IMAGE_RE.findall(text)]

    async def _download_images(self, urls: list[str]) -> list[str]:
        attachments = self._attachments
        if attachments is None:
            return []

        def download(url: str) -> str:
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme not in ("http", "https"):
                raise ValueError("Unsupported QQ image URL")
            request = urllib.request.Request(url, headers={"User-Agent": "kirakira-agent/0.1"})
            with urllib.request.urlopen(request, timeout=30) as response:
                declared = int(response.headers.get("content-length") or "0")
                if declared > 20 * 1024 * 1024:
                    raise ValueError("QQ image exceeds 20 MB")
                data = response.read(20 * 1024 * 1024 + 1)
            if len(data) > 20 * 1024 * 1024:
                raise ValueError("QQ image exceeds 20 MB")
            suffix = Path(parsed.path).suffix.lower()
            if not suffix or len(suffix) > 12:
                suffix = ".jpg"
            return str(attachments.write_bytes(data, prefix="image_", suffix=suffix))

        results = await asyncio.gather(
            *(asyncio.to_thread(download, url) for url in urls[:8]),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                logger.warning("[qq] image download failed: %s", result)
        return [str(result) for result in results if not isinstance(result, Exception)]

    async def _send_media(self, msg: OutboundMessage, *, group: bool) -> None:
        action = "send_group_msg" if group else "send_private_msg"
        target = "group_id" if group else "user_id"
        target_id = msg.chat_id[len(_GROUP_PREFIX):] if group else msg.chat_id
        for item in msg.media:
            path = Path(item).resolve()
            if not path.is_file():
                continue
            kind = "image" if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp"} else "file"
            cq = "[CQ:%s,file=file://%s]" % (kind, urllib.parse.quote(str(path)))
            await asyncio.to_thread(
                self._api,
                action,
                {target: target_id, "message": cq},
            )
