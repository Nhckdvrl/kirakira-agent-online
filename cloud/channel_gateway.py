"""Multi-tenant channel gateway with durable identity and outbound delivery."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hmac
import json
import logging
import os
import socket
from pathlib import Path
import mimetypes
import base64
import re
from uuid import UUID, uuid4
from typing import Annotated, AsyncIterator, Any

from fastapi import FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel, Field
import httpx
import websockets

from cloud.database import CloudSettings, build_engine, build_session_factory
from cloud.logging import safe_exception_summary
from cloud.mcp import validate_remote_mcp_url
from cloud.store import CloudStore, StoreConflictError, StoreNotFoundError, StoreStateError
from agent.tools.execution_backend import RemoteSandboxExecutionBackend


logger = logging.getLogger("kirakira.cloud.channels")


def _onebot_base_url() -> str:
    value = os.getenv("KIRAKIRA_QQ_API_BASE_URL", "").rstrip("/")
    if not value:
        raise RuntimeError("KIRAKIRA_QQ_API_BASE_URL is not configured")
    return value


def _onebot_headers() -> dict[str, str]:
    token = os.getenv("KIRAKIRA_QQ_ACCESS_TOKEN", "")
    return {"Authorization": f"Bearer {token}"} if token else {}


def _onebot_target(chat_id: str) -> tuple[str, str, str]:
    if chat_id.startswith("gqq:"):
        return "send_group_msg", "group_id", chat_id.removeprefix("gqq:")
    return "send_private_msg", "user_id", chat_id


_CQ_TAG_RE = re.compile(r"\[CQ:[^\]]+\]")


def _parse_onebot_message(payload: dict[str, Any]) -> tuple[str, list[str]]:
    message = payload.get("message")
    if isinstance(message, list):
        text: list[str] = []
        images: list[str] = []
        for segment in message:
            if not isinstance(segment, dict) or not isinstance(segment.get("data"), dict):
                continue
            data = segment["data"]
            if segment.get("type") == "text":
                text.append(str(data.get("text") or ""))
            elif segment.get("type") == "image" and data.get("url"):
                images.append(str(data["url"]))
        return "".join(text).strip(), images
    return _CQ_TAG_RE.sub("", str(payload.get("raw_message") or message or "")).strip(), []


class PairIn(BaseModel):
    provider: str
    code: str = Field(min_length=10, max_length=200)
    external_user_id: str = Field(min_length=1, max_length=300)
    external_chat_id: str = Field(min_length=1, max_length=300)
    display_name: str = Field(default="", max_length=300)


class InboundIn(BaseModel):
    provider: str
    external_event_id: str = Field(min_length=1, max_length=300)
    external_chat_id: str = Field(min_length=1, max_length=300)
    content: str = Field(min_length=1, max_length=1_000_000)


class TelegramPollingAdapter:
    def __init__(
        self,
        store: CloudStore,
        token: str,
        workspace: RemoteSandboxExecutionBackend,
    ) -> None:
        self.store = store
        self.token = token
        self.workspace = workspace
        self.client = httpx.AsyncClient(timeout=35)
        self._stop = asyncio.Event()
        self._offset = 0

    async def run_forever(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    response = await self.client.get(
                        f"https://api.telegram.org/bot{self.token}/getUpdates",
                        params={"offset": self._offset, "timeout": 25, "allowed_updates": '["message"]'},
                    )
                    response.raise_for_status()
                    for update in response.json().get("result", []):
                        self._offset = max(self._offset, int(update.get("update_id", 0)) + 1)
                        await self._handle(update)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("Telegram polling failed: %s", safe_exception_summary(exc))
                    await asyncio.sleep(2)
        finally:
            await self.client.aclose()

    async def _handle(self, update: dict[str, Any]) -> None:
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        sender = message.get("from") or {}
        chat_id = str(chat.get("id") or "")
        if not chat_id:
            return
        text = str(message.get("text") or message.get("caption") or "").strip()
        if text.startswith("/link "):
            try:
                await self.store.consume_channel_pairing(
                    text.split(None, 1)[1].strip(),
                    provider="telegram",
                    external_user_id=str(sender.get("id") or ""),
                    external_chat_id=chat_id,
                    display_name=str(sender.get("username") or sender.get("first_name") or ""),
                )
            except (StoreStateError, StoreConflictError) as exc:
                await self._reply(chat_id, f"配对失败：{exc}")
            else:
                await self._reply(chat_id, "Kirakira Cloud 配对成功。之后的消息会进入所选对话。")
            return
        try:
            link = await self.store.resolve_channel_link("telegram", chat_id)
        except StoreNotFoundError:
            await self._reply(chat_id, "此聊天尚未配对。请先在 Kirakira Web 生成配对码，再发送 /link 配对码。")
            return
        file_ids = []
        attachments = []
        photos = message.get("photo") or []
        if photos:
            attachments.append((photos[-1].get("file_id"), "photo.jpg", "image/jpeg"))
        document = message.get("document") or {}
        if document.get("file_id"):
            attachments.append(
                (
                    document["file_id"],
                    str(document.get("file_name") or "document.bin"),
                    str(document.get("mime_type") or "application/octet-stream"),
                )
            )
        for file_id, filename, content_type in attachments:
            content = await self._download(str(file_id))
            if len(content) > 16_000_000:
                await self._reply(chat_id, f"附件 {filename} 超过 16 MB，未接收。")
                continue
            safe_name = Path(filename).name.replace("/", "_")
            identifier = uuid4()
            workspace_path = f"uploads/{link.conversation_id}/{identifier.hex}/{safe_name}"
            await self.workspace.write_binary(str(link.conversation_id), workspace_path, content)
            saved = await self.store.create_user_file(
                link.user_id,
                link.conversation_id,
                workspace_path=workspace_path,
                filename=safe_name,
                content_type=content_type,
                size_bytes=len(content),
                sha256_hex=__import__("hashlib").sha256(content).hexdigest(),
            )
            file_ids.append(saved.id)
        if not text and file_ids:
            text = "请查看我发送的附件。"
        if not text:
            return
        await self.store.ingest_channel_message(
            provider="telegram",
            external_event_id=str(update.get("update_id")),
            external_chat_id=chat_id,
            content=text,
            file_ids=file_ids,
        )

    async def _download(self, file_id: str) -> bytes:
        metadata = await self.client.get(
            f"https://api.telegram.org/bot{self.token}/getFile", params={"file_id": file_id}
        )
        metadata.raise_for_status()
        path = metadata.json()["result"]["file_path"]
        response = await self.client.get(
            f"https://api.telegram.org/file/bot{self.token}/{path}"
        )
        response.raise_for_status()
        return response.content

    async def _reply(self, chat_id: str, text: str) -> None:
        response = await self.client.post(
            f"https://api.telegram.org/bot{self.token}/sendMessage",
            json={"chat_id": chat_id, "text": text[:4096]},
        )
        response.raise_for_status()

    def stop(self) -> None:
        self._stop.set()


class QQBotGatewayAdapter:
    """Tencent official C2C Gateway with the original token/heartbeat semantics."""

    def __init__(self, store: CloudStore, app_id: str, client_secret: str) -> None:
        self.store = store
        self.app_id = app_id.strip()
        self.client_secret = client_secret.strip()
        self.api_base_url = os.getenv(
            "KIRAKIRA_QQBOT_API_BASE_URL", "https://api.sgroup.qq.com"
        ).rstrip("/")
        self.client = httpx.AsyncClient(timeout=30)
        self._token = ""
        self._token_expires_at = 0.0
        self._token_lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._sequence: int | None = None
        self.latest_message_id: dict[str, str] = {}

    async def _access_token(self, *, force: bool = False) -> str:
        async with self._token_lock:
            now = asyncio.get_running_loop().time()
            if not force and self._token and now < self._token_expires_at:
                return self._token
            response = await self.client.post(
                "https://bots.qq.com/app/getAppAccessToken",
                json={"appId": self.app_id, "clientSecret": self.client_secret},
            )
            response.raise_for_status()
            data = response.json()
            token = str(data.get("access_token") or "")
            if not token:
                raise RuntimeError("QQBot access token is missing")
            self._token = token
            self._token_expires_at = now + max(30, int(data.get("expires_in") or 7200) - 60)
            return token

    async def _gateway_url(self) -> str:
        token = await self._access_token()
        response = await self.client.get(
            f"{self.api_base_url}/gateway",
            headers={"Authorization": f"QQBot {token}"},
        )
        response.raise_for_status()
        url = str(response.json().get("url") or "")
        if not url:
            raise RuntimeError("QQBot gateway URL is missing")
        return url

    async def run_forever(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    await self._gateway_session()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("QQBot gateway failed: %s", safe_exception_summary(exc))
                    await asyncio.sleep(2)
        finally:
            await self.client.aclose()

    async def _gateway_session(self) -> None:
        async with websockets.connect(await self._gateway_url(), open_timeout=10) as websocket:
            heartbeat: asyncio.Task[None] | None = None
            try:
                async for raw in websocket:
                    payload = json.loads(raw)
                    opcode = int(payload.get("op", -1))
                    if opcode == 10:
                        interval = int((payload.get("d") or {}).get("heartbeat_interval") or 45000)
                        token = await self._access_token()
                        await websocket.send(
                            json.dumps(
                                {
                                    "op": 2,
                                    "d": {
                                        "token": f"QQBot {token}",
                                        "intents": 1 << 25,
                                        "shard": [0, 1],
                                    },
                                }
                            )
                        )
                        heartbeat = asyncio.create_task(self._heartbeat(websocket, interval / 1000))
                    elif opcode == 0:
                        if payload.get("s") is not None:
                            self._sequence = int(payload["s"])
                        if payload.get("t") == "C2C_MESSAGE_CREATE":
                            await self._handle_c2c(payload.get("d") or {})
                    elif opcode in {7, 9}:
                        return
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)

    async def _heartbeat(self, websocket: Any, interval: float) -> None:
        while True:
            await asyncio.sleep(max(1, interval))
            await websocket.send(json.dumps({"op": 1, "d": self._sequence}))

    async def _handle_c2c(self, data: dict[str, Any]) -> None:
        author = data.get("author") or {}
        openid = str(author.get("user_openid") or data.get("user_openid") or "")
        message_id = str(data.get("id") or "")
        content = str(data.get("content") or "").strip()
        if not openid or not message_id or not content:
            return
        chat_id = "c2c:" + openid
        self.latest_message_id[chat_id] = message_id
        if content.startswith("/link "):
            try:
                await self.store.consume_channel_pairing(
                    content.split(None, 1)[1].strip(),
                    provider="qqbot",
                    external_user_id=openid,
                    external_chat_id=chat_id,
                    display_name=openid,
                )
            except (StoreStateError, StoreConflictError) as exc:
                await self.send_text(chat_id, f"配对失败：{exc}", reply_to=message_id)
            else:
                await self.send_text(chat_id, "Kirakira Cloud 配对成功。", reply_to=message_id)
            return
        try:
            await self.store.ingest_channel_message(
                provider="qqbot",
                external_event_id=message_id,
                external_chat_id=chat_id,
                content=content,
            )
        except StoreNotFoundError:
            await self.send_text(
                chat_id,
                "此聊天尚未配对。请先在 Kirakira Web 生成配对码。",
                reply_to=message_id,
            )

    async def send_text(self, chat_id: str, content: str, *, reply_to: str = "") -> None:
        openid = chat_id.removeprefix("c2c:")
        token = await self._access_token()
        body: dict[str, Any] = {"content": content or "(empty)", "msg_type": 0}
        if reply_to:
            body["msg_id"] = reply_to
        response = await self.client.post(
            f"{self.api_base_url}/v2/users/{openid}/messages",
            headers={"Authorization": f"QQBot {token}", "X-Union-Appid": self.app_id},
            json=body,
        )
        if response.status_code == 401:
            token = await self._access_token(force=True)
            response = await self.client.post(
                f"{self.api_base_url}/v2/users/{openid}/messages",
                headers={"Authorization": f"QQBot {token}", "X-Union-Appid": self.app_id},
                json=body,
            )
        response.raise_for_status()

    def stop(self) -> None:
        self._stop.set()


class ChannelDeliveryWorker:
    def __init__(
        self,
        store: CloudStore,
        *,
        worker_id: str,
        workspace: RemoteSandboxExecutionBackend | None = None,
        qqbot: QQBotGatewayAdapter | None = None,
    ) -> None:
        self.store = store
        self.worker_id = worker_id[:200]
        self.workspace = workspace
        self.qqbot = qqbot
        self.client = httpx.AsyncClient(timeout=30)
        self._stop = asyncio.Event()

    async def run_once(self) -> bool:
        claimed = await self.store.claim_channel_delivery(self.worker_id)
        if claimed is None:
            return False
        delivery, link, message = claimed
        try:
            await self._send(link.provider, link.external_chat_id, message.content)
            await self._send_media(
                link.provider,
                link.external_chat_id,
                str(message.conversation_id),
                list((message.agent_metadata or {}).get("media") or []),
            )
        except Exception as exc:  # noqa: BLE001 - durable retry evidence
            await self.store.finish_channel_delivery(
                delivery.id, self.worker_id, sent=False, error=safe_exception_summary(exc)
            )
        else:
            await self.store.finish_channel_delivery(delivery.id, self.worker_id, sent=True)
        return True

    async def _send(self, provider: str, chat_id: str, content: str) -> None:
        if provider == "telegram":
            token = os.getenv("KIRAKIRA_TELEGRAM_BOT_TOKEN", "")
            if not token:
                raise RuntimeError("Telegram bot token is not configured")
            response = await self.client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": content[:4096]},
            )
        elif provider == "qq":
            action, target, target_id = _onebot_target(chat_id)
            response = await self.client.post(
                f"{_onebot_base_url()}/{action}",
                headers=_onebot_headers(),
                json={target: target_id, "message": content},
            )
        elif provider == "qqbot":
            if self.qqbot is None:
                raise RuntimeError("QQBot gateway is not configured")
            await self.qqbot.send_text(
                chat_id,
                content,
                reply_to=self.qqbot.latest_message_id.get(chat_id, ""),
            )
            return
        else:
            raise RuntimeError(f"unsupported channel provider: {provider}")
        response.raise_for_status()
        data = response.json()
        if data.get("ok") is False or int(data.get("retcode") or 0) != 0:
            raise RuntimeError(f"{provider} rejected delivery")

    async def _send_media(
        self, provider: str, chat_id: str, owner_id: str, media: list[str]
    ) -> None:
        if not media:
            return
        if self.workspace is None:
            raise RuntimeError("channel attachment delivery requires the sandbox workspace")
        for source in media:
            payload = await self.workspace.read_binary(owner_id, str(source))
            filename = Path(str(source)).name or "attachment.bin"
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            if provider == "telegram":
                token = os.getenv("KIRAKIRA_TELEGRAM_BOT_TOKEN", "")
                method = "sendPhoto" if content_type.startswith("image/") else "sendDocument"
                field = "photo" if method == "sendPhoto" else "document"
                response = await self.client.post(
                    f"https://api.telegram.org/bot{token}/{method}",
                    data={"chat_id": chat_id},
                    files={field: (filename, payload, content_type)},
                )
            elif provider == "qq":
                action, target, target_id = _onebot_target(chat_id)
                response = await self.client.post(
                    f"{_onebot_base_url()}/{action}",
                    headers=_onebot_headers(),
                    json={
                        target: target_id,
                        "message": "[CQ:%s,file=base64://%s]"
                        % (
                            "image" if content_type.startswith("image/") else "file",
                            base64.b64encode(payload).decode("ascii"),
                        ),
                    },
                )
            else:
                raise RuntimeError("QQBot does not support outbound attachments")
            response.raise_for_status()
            result = response.json()
            if result.get("ok") is False or int(result.get("retcode") or 0) != 0:
                raise RuntimeError(f"{provider} rejected attachment delivery")

    async def run_forever(self) -> None:
        try:
            while not self._stop.is_set():
                if await self.run_once():
                    continue
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=1)
                except TimeoutError:
                    pass
        finally:
            await self.client.aclose()

    def stop(self) -> None:
        self._stop.set()


def create_app() -> FastAPI:
    token = os.getenv("KIRAKIRA_CHANNEL_TOKEN", "")
    if len(token) < 24:
        raise RuntimeError("KIRAKIRA_CHANNEL_TOKEN must contain at least 24 characters")
    settings = CloudSettings.from_env()
    engine = build_engine(settings, pool_pre_ping=True)
    store = CloudStore(build_session_factory(engine))
    telegram_token = os.getenv("KIRAKIRA_TELEGRAM_BOT_TOKEN", "")
    qqbot_app_id = os.getenv("KIRAKIRA_QQBOT_APP_ID", "")
    qqbot_secret = os.getenv("KIRAKIRA_QQBOT_CLIENT_SECRET", "")
    workspace: RemoteSandboxExecutionBackend | None = None
    telegram: TelegramPollingAdapter | None = None
    qqbot: QQBotGatewayAdapter | None = None
    if os.getenv("KIRAKIRA_SANDBOX_URL", ""):
        workspace = RemoteSandboxExecutionBackend(
            os.environ.get("KIRAKIRA_SANDBOX_URL", ""),
            auth_token=os.getenv("KIRAKIRA_SANDBOX_TOKEN", ""),
        )
    if telegram_token:
        if workspace is None:
            raise RuntimeError("Telegram attachments require KIRAKIRA_SANDBOX_URL")
        telegram = TelegramPollingAdapter(store, telegram_token, workspace)
    if qqbot_app_id or qqbot_secret:
        if not qqbot_app_id or not qqbot_secret:
            raise RuntimeError("both KIRAKIRA_QQBOT_APP_ID and CLIENT_SECRET are required")
        qqbot = QQBotGatewayAdapter(store, qqbot_app_id, qqbot_secret)
    delivery = ChannelDeliveryWorker(
        store,
        worker_id=f"{socket.gethostname()}:{os.getpid()}:channel",
        workspace=workspace,
        qqbot=qqbot,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if workspace is not None:
            await workspace.probe()
        if qqbot is not None:
            await qqbot._access_token(force=True)
        tasks = [asyncio.create_task(delivery.run_forever())]
        if telegram is not None:
            tasks.append(asyncio.create_task(telegram.run_forever()))
        if qqbot is not None:
            tasks.append(asyncio.create_task(qqbot.run_forever()))
        try:
            yield
        finally:
            delivery.stop()
            if telegram is not None:
                telegram.stop()
            if qqbot is not None:
                qqbot.stop()
            await asyncio.gather(*tasks)
            if workspace is not None:
                await workspace.shutdown()
            await engine.dispose()

    app = FastAPI(title="Kirakira Cloud Channel Gateway", lifespan=lifespan)

    def auth(authorization: Annotated[str | None, Header()] = None) -> None:
        if not hmac.compare_digest(authorization or "", f"Bearer {token}"):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid channel credential")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/pair")
    async def pair(payload: PairIn, authorization: Annotated[str | None, Header()] = None):
        auth(authorization)
        try:
            link = await store.consume_channel_pairing(
                payload.code,
                provider=payload.provider,
                external_user_id=payload.external_user_id,
                external_chat_id=payload.external_chat_id,
                display_name=payload.display_name,
            )
        except (StoreStateError, StoreConflictError) as exc:
            raise HTTPException(409, str(exc)) from exc
        return {"linked": True, "conversation_id": str(link.conversation_id)}

    @app.post("/v1/inbound")
    async def inbound(payload: InboundIn, authorization: Annotated[str | None, Header()] = None):
        auth(authorization)
        try:
            _, run, accepted = await store.ingest_channel_message(**payload.model_dump())
        except StoreNotFoundError as exc:
            raise HTTPException(404, str(exc)) from exc
        return {"accepted": accepted, "run_id": str(run.id)}

    @app.post("/v1/qq/onebot")
    async def onebot_webhook(
        request: Request,
        authorization: Annotated[str | None, Header()] = None,
    ):
        qq_token = os.getenv("KIRAKIRA_QQ_ACCESS_TOKEN", "")
        expected = f"Bearer {qq_token or token}"
        if not hmac.compare_digest(authorization or "", expected):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid QQ credential")
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(422, "OneBot payload must be an object")
        if payload.get("post_type") not in (None, "", "message"):
            return {"ok": True, "ignored": "post_type"}
        user_id = str(payload.get("user_id") or "")
        group_id = str(payload.get("group_id") or "")
        chat_id = f"gqq:{group_id}" if payload.get("message_type") == "group" else user_id
        content, image_urls = _parse_onebot_message(payload)
        if not chat_id:
            return {"ok": True, "ignored": "missing_identity"}
        if content.startswith("/link "):
            try:
                link = await store.consume_channel_pairing(
                    content.split(None, 1)[1].strip(),
                    provider="qq",
                    external_user_id=user_id,
                    external_chat_id=chat_id,
                    display_name=str((payload.get("sender") or {}).get("nickname") or ""),
                )
            except (StoreStateError, StoreConflictError) as exc:
                return {"ok": False, "error": str(exc)}
            return {"ok": True, "linked": True, "conversation_id": str(link.conversation_id)}
        try:
            link = await store.resolve_channel_link("qq", chat_id)
        except StoreNotFoundError as exc:
            raise HTTPException(404, "QQ chat is not linked") from exc
        file_ids: list[UUID] = []
        if image_urls and workspace is None:
            raise HTTPException(503, "QQ attachment ingestion requires sandbox workspace")
        for position, url in enumerate(image_urls[:10]):
            if not url.startswith("https://"):
                raise HTTPException(422, "QQ image URL must use HTTPS")
            try:
                safe_url = validate_remote_mcp_url(url)
            except ValueError as exc:
                raise HTTPException(422, "QQ image URL is not a public HTTPS URL") from exc
            response = await delivery.client.get(safe_url)
            response.raise_for_status()
            if len(response.content) > 16_000_000:
                raise HTTPException(413, "QQ image exceeds 16 MB")
            identifier = uuid4()
            filename = f"qq-image-{position + 1}.jpg"
            workspace_path = (
                f"uploads/{link.conversation_id}/{identifier.hex}/{filename}"
            )
            await workspace.write_binary(
                str(link.conversation_id), workspace_path, response.content
            )
            saved = await store.create_user_file(
                link.user_id,
                link.conversation_id,
                workspace_path=workspace_path,
                filename=filename,
                content_type=response.headers.get("content-type", "image/jpeg")[:200],
                size_bytes=len(response.content),
                sha256_hex=__import__("hashlib").sha256(response.content).hexdigest(),
            )
            file_ids.append(saved.id)
        if not content and file_ids:
            content = "[用户发送了图片]"
        if not content:
            return {"ok": True, "ignored": "empty"}
        _, run, accepted = await store.ingest_channel_message(
            provider="qq",
            external_event_id=str(payload.get("message_id") or uuid4()),
            external_chat_id=chat_id,
            content=content,
            file_ids=file_ids,
        )
        return {"ok": True, "accepted": accepted, "run_id": str(run.id)}

    app.state.store = store
    app.state.delivery_worker = delivery
    return app
