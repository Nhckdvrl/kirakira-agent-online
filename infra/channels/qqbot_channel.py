"""Official Tencent QQBot C2C channel, aligned with Reference setup semantics."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import httpx
import websockets

from infra.channels.base import MessageDeduper
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
_C2C_PREFIX = "c2c:"
_C2C_INTENT = 1 << 25


class QQBotChannel:
    """Tencent official bot over Gateway WebSocket + v2 C2C HTTP API."""

    def __init__(
        self,
        *,
        app_id: str,
        client_secret: str,
        allow_from: list[str] | None = None,
        channel_name: str = "qqbot",
        api_base_url: str = "https://api.sgroup.qq.com",
    ) -> None:
        self.name = channel_name
        self.app_id = str(app_id).strip()
        self.client_secret = str(client_secret).strip()
        self.allow_from = {
            str(item).removeprefix(_C2C_PREFIX).strip()
            for item in (allow_from or [])
            if str(item).strip()
        }
        self.api_base_url = api_base_url.rstrip("/")
        self._ctx: ChannelContext | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._token = ""
        self._token_expires_at = 0.0
        self._token_lock = asyncio.Lock()
        self._sequence: int | None = None
        self._connected = asyncio.Event()
        self._latest_message_id: dict[str, str] = {}
        self._deduper = MessageDeduper()
        self._push_registration = None

    async def start(self, ctx: ChannelContext) -> None:
        if not self.app_id or not self.client_secret:
            raise RuntimeError("QQBot app_id/client_secret is required")
        self._ctx = ctx
        await self._access_token(force=True)
        if ctx.push_tool is not None:
            self._push_registration = ctx.push_tool.register_channel(
                self.name, self._deliver_message
            )
        ctx.bus.subscribe_outbound(self.name, self._on_response)
        self._running = True
        self._task = asyncio.create_task(self._run_forever(), name="qqbot_gateway")
        try:
            async with asyncio.timeout(15):
                await self._connected.wait()
        except TimeoutError as exc:
            await self.stop()
            raise RuntimeError("QQBot gateway did not become ready") from exc
        ctx.log.info("official QQBot channel started")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._ctx is not None:
            self._ctx.bus.unsubscribe_outbound(self.name, self._on_response)
        if self._push_registration is not None:
            self._push_registration.close()
            self._push_registration = None
        self._task = None
        self._connected.clear()
        self._ctx = None

    async def _access_token(self, *, force: bool = False) -> str:
        async with self._token_lock:
            if not force and self._token and time.monotonic() < self._token_expires_at:
                return self._token
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    "https://bots.qq.com/app/getAppAccessToken",
                    json={"appId": self.app_id, "clientSecret": self.client_secret},
                )
                response.raise_for_status()
                payload = response.json()
            token = str(payload.get("access_token") or "").strip()
            if not token:
                raise RuntimeError("QQBot access token missing: %s" % payload)
            expires_in = max(60, int(payload.get("expires_in") or 7200))
            self._token = token
            self._token_expires_at = time.monotonic() + max(30, expires_in - 60)
            return token

    async def _gateway_url(self) -> str:
        token = await self._access_token()
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"{self.api_base_url}/gateway",
                headers={"Authorization": f"QQBot {token}"},
            )
            response.raise_for_status()
            url = str(response.json().get("url") or "").strip()
        if not url:
            raise RuntimeError("QQBot gateway URL missing")
        return url

    async def _run_forever(self) -> None:
        while self._running:
            try:
                await self._gateway_session()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[qqbot] gateway disconnected: %s", exc)
            if self._running:
                await asyncio.sleep(2)

    async def _gateway_session(self) -> None:
        url = await self._gateway_url()
        async with websockets.connect(url, open_timeout=10) as websocket:
            heartbeat: asyncio.Task[None] | None = None
            try:
                async for raw in websocket:
                    payload = json.loads(raw)
                    opcode = int(payload.get("op", -1))
                    if opcode == 10:
                        interval_ms = int((payload.get("d") or {}).get("heartbeat_interval") or 45000)
                        token = await self._access_token()
                        await websocket.send(
                            json.dumps(
                                {
                                    "op": 2,
                                    "d": {
                                        "token": f"QQBot {token}",
                                        "intents": _C2C_INTENT,
                                        "shard": [0, 1],
                                    },
                                }
                            )
                        )
                        self._connected.set()
                        heartbeat = asyncio.create_task(
                            self._heartbeat(websocket, interval_ms / 1000),
                            name="qqbot_heartbeat",
                        )
                    elif opcode == 0:
                        if payload.get("s") is not None:
                            self._sequence = int(payload["s"])
                        if payload.get("t") == "C2C_MESSAGE_CREATE":
                            await self._handle_c2c(payload.get("d") or {})
                    elif opcode in {7, 9}:
                        return
            finally:
                self._connected.clear()
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)

    async def _heartbeat(self, websocket: Any, interval_s: float) -> None:
        while True:
            await asyncio.sleep(max(1.0, interval_s))
            await websocket.send(json.dumps({"op": 1, "d": self._sequence}))

    async def _handle_c2c(self, data: dict[str, Any]) -> None:
        ctx = self._ctx
        if ctx is None:
            return
        author = data.get("author") or {}
        openid = str(author.get("user_openid") or data.get("user_openid") or "").strip()
        message_id = str(data.get("id") or "").strip()
        content = str(data.get("content") or "").strip()
        if not openid or not message_id or not content:
            return
        if self.allow_from and openid not in self.allow_from:
            logger.warning("[qqbot] unauthorized user ignored: %s", openid)
            return
        if self._deduper.seen(f"{openid}:{message_id}"):
            return
        chat_id = _C2C_PREFIX + openid
        self._latest_message_id[chat_id] = message_id
        if content.lower() in {"/stop", "stop"}:
            interrupted = bool(
                ctx.interrupt and ctx.interrupt(f"{self.name}:{chat_id}")
            )
            await self.send_text(
                chat_id,
                "本轮已中断。" if interrupted else "当前没有正在执行的任务。",
                reply_to=message_id,
            )
            return
        await ctx.bus.publish_inbound(
            InboundMessage(
                channel=self.name,
                sender=openid,
                chat_id=chat_id,
                content=content,
                metadata={
                    "qqbot_message_id": message_id,
                    "user_openid": openid,
                },
            )
        )

    async def _on_response(self, message: OutboundMessage) -> None:
        receipt = await self._deliver_message(channel_message_from_outbound(message))
        if not receipt.succeeded:
            raise RuntimeError(receipt.detail or "QQBot 消息提交失败")

    async def _deliver_message(self, message: ChannelMessage) -> DeliveryReceipt:
        async def send_text(chat_id: str, content: str) -> None:
            await self.send_text(
                chat_id,
                content,
                reply_to=self._latest_message_id.get(chat_id, ""),
            )

        async def unsupported_file(
            chat_id: str, source: str, filename: str | None = None
        ) -> None:
            _ = (chat_id, source, filename)
            raise RuntimeError("QQBot 当前不支持文件附件")

        async def unsupported_image(chat_id: str, source: str) -> None:
            _ = (chat_id, source)
            raise RuntimeError("QQBot 当前不支持图片附件")

        return await deliver_message_parts(
            message,
            send_text=send_text,
            send_file=unsupported_file,
            send_image=unsupported_image,
        )

    async def send_text(self, chat_id: str, content: str, *, reply_to: str = "") -> None:
        openid = str(chat_id).removeprefix(_C2C_PREFIX).strip()
        if not openid:
            raise ValueError("QQBot chat_id must be c2c:USER_OPENID")
        token = await self._access_token()
        payload: dict[str, Any] = {"content": content or "(empty)", "msg_type": 0}
        if reply_to:
            payload["msg_id"] = reply_to
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.api_base_url}/v2/users/{openid}/messages",
                headers={
                    "Authorization": f"QQBot {token}",
                    "X-Union-Appid": self.app_id,
                },
                json=payload,
            )
            if response.status_code == 401:
                token = await self._access_token(force=True)
                response = await client.post(
                    f"{self.api_base_url}/v2/users/{openid}/messages",
                    headers={
                        "Authorization": f"QQBot {token}",
                        "X-Union-Appid": self.app_id,
                    },
                    json=payload,
                )
            response.raise_for_status()
