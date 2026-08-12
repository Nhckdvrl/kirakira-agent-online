"""FastAPI surface for the durable multi-user Cloud application."""

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import json
import logging
import time
import hashlib
import os
from pathlib import Path
import re
import secrets
import httpx
from typing import Annotated
from uuid import UUID
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from cloud.database import CloudSettings, build_engine, build_session_factory
from cloud.models import User
from cloud.schemas import (
    ConversationCreate,
    ConversationOut,
    AutomationConfigIn,
    AutomationOut,
    CredentialsIn,
    MessageCreate,
    MessageOut,
    RunAccepted,
    RunEventOut,
    RunOut,
    ProactiveEventIn,
    ProactiveEventAccepted,
    ScheduleCreate,
    ScheduleOut,
    FileOut,
    FileUploadIn,
    ChannelPairingIn,
    ChannelPairingOut,
    ChannelLinkOut,
    McpServerCreate,
    McpServerOut,
    PluginCreate,
    PluginOut,
    SubagentJobOut,
    SkillCreate,
    SkillOut,
    UserOut,
)
from agent.tools.execution_backend import RemoteSandboxExecutionBackend
from cloud.credentials import CredentialVault
from cloud.mcp import validate_remote_mcp_url
from cloud.plugins import validate_plugin_manifest, validate_remote_plugin_url
from cloud.skills import parse_skill_document
from cloud.scheduler import create_cloud_schedule
from cloud.security import new_session_token
from cloud.observability import HTTP_DURATION, HTTP_REQUESTS, RATE_LIMITED
from cloud.store import (
    CloudStore,
    StoreConflictError,
    StoreNotFoundError,
    StoreStateError,
)


SESSION_COOKIE = "kirakira_session"
logger = logging.getLogger("kirakira.cloud.api")


def create_app(
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    settings: CloudSettings | None = None,
    workspace_backend: RemoteSandboxExecutionBackend | None = None,
) -> FastAPI:
    resolved_settings = settings
    engine: AsyncEngine | None = None
    if session_factory is None:
        resolved_settings = resolved_settings or CloudSettings.from_env()
        engine = build_engine(resolved_settings, pool_pre_ping=True)
        session_factory = build_session_factory(engine)
    else:
        resolved_settings = resolved_settings or CloudSettings(
            database_url="injected://", session_cookie_secure=False
        )
    store = CloudStore(session_factory)
    sandbox = workspace_backend
    if sandbox is None and os.getenv("KIRAKIRA_SANDBOX_URL", "").strip():
        sandbox = RemoteSandboxExecutionBackend(
            os.environ["KIRAKIRA_SANDBOX_URL"],
            auth_token=os.getenv("KIRAKIRA_SANDBOX_TOKEN", ""),
            timeout_seconds=float(os.getenv("KIRAKIRA_SANDBOX_TIMEOUT_SECONDS", "30")),
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if sandbox is not None:
            await sandbox.probe()
        try:
            yield
        finally:
            if sandbox is not None:
                await sandbox.shutdown()
            if engine is not None:
                await engine.dispose()

    app = FastAPI(title="Kirakira Cloud API", version="0.1.0", lifespan=lifespan)
    app.state.store = store
    app.state.workspace_backend = sandbox

    allowed_origins = set(resolved_settings.allowed_origins)
    if allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=sorted(allowed_origins),
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=[
                "Content-Type",
                "Idempotency-Key",
                "Last-Event-ID",
            ],
        )

    def _secure_response(response: Response, request_id: str) -> Response:
        response.headers["X-Request-ID"] = request_id
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'none'; script-src 'self'; style-src 'self'; "
            "connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'",
        )
        return response

    @app.middleware("http")
    async def browser_security(request: Request, call_next):
        request_id = uuid4().hex
        started_at = time.monotonic()
        origin = (request.headers.get("origin") or "").rstrip("/")
        if origin and origin not in allowed_origins:
            return _secure_response(
                Response(status_code=status.HTTP_403_FORBIDDEN), request_id
            )
        if (
            resolved_settings.session_cookie_secure
            and request.method in {"POST", "PUT", "PATCH", "DELETE"}
            and SESSION_COOKIE in request.cookies
            and not origin
        ):
            return _secure_response(
                Response(status_code=status.HTTP_403_FORBIDDEN), request_id
            )
        try:
            response = await call_next(request)
        except Exception:
            route = getattr(request.scope.get("route"), "path", "unmatched")
            HTTP_REQUESTS.labels(request.method, route, "500").inc()
            HTTP_DURATION.labels(request.method, route).observe(
                max(0.0, time.monotonic() - started_at)
            )
            logger.exception(
                "http request failed",
                extra={
                    "cloud_fields": {
                        "request_id": request_id,
                        "method": request.method,
                        "route": route,
                        "status": 500,
                    }
                },
            )
            raise
        route = getattr(request.scope.get("route"), "path", "unmatched")
        elapsed = max(0.0, time.monotonic() - started_at)
        HTTP_REQUESTS.labels(request.method, route, str(response.status_code)).inc()
        HTTP_DURATION.labels(request.method, route).observe(elapsed)
        _secure_response(response, request_id)
        logger.info(
            "http request",
            extra={
                "cloud_fields": {
                    "request_id": request_id,
                    "method": request.method,
                    "route": route,
                    "status": response.status_code,
                    "duration_ms": round(elapsed * 1000, 2),
                }
            },
        )
        return response

    async def current_user(
        token: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
    ) -> User:
        user = await store.user_for_token(token or "")
        if user is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "authentication required")
        return user

    def set_session_cookie(response: Response, token: str) -> None:
        response.set_cookie(
            SESSION_COOKIE,
            token,
            max_age=resolved_settings.session_ttl_seconds,
            httponly=True,
            secure=resolved_settings.session_cookie_secure,
            samesite="lax",
            path="/",
        )

    async def consume_or_reject(
        subject: str, scope: str, *, limit: int
    ) -> None:
        decision = await store.consume_rate_limit(
            subject, scope, limit=limit, window_seconds=60
        )
        if not decision.allowed:
            RATE_LIMITED.labels(scope).inc()
            raise HTTPException(
                status.HTTP_429_TOO_MANY_REQUESTS,
                "rate limit exceeded",
                headers={"Retry-After": str(decision.retry_after_seconds)},
            )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> dict[str, str]:
        try:
            await store.ping()
        except Exception as exc:
            raise HTTPException(503, "database unavailable") from exc
        return {"status": "ready"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    static_root = Path(__file__).with_name("static")

    @app.get("/", include_in_schema=False)
    async def cloud_ui() -> Response:
        return FileResponse(static_root / "index.html")

    @app.get("/app.js", include_in_schema=False)
    async def cloud_ui_js() -> Response:
        return FileResponse(static_root / "app.js", media_type="text/javascript")

    @app.get("/app.css", include_in_schema=False)
    async def cloud_ui_css() -> Response:
        return FileResponse(static_root / "app.css", media_type="text/css")

    @app.get("/settings.css", include_in_schema=False)
    async def cloud_ui_settings_css() -> Response:
        return FileResponse(static_root / "settings.css", media_type="text/css")

    @app.post("/v1/auth/register", response_model=UserOut, status_code=201)
    async def register(
        payload: CredentialsIn, response: Response, request: Request
    ) -> User:
        client_host = request.client.host if request.client is not None else "unknown"
        await consume_or_reject(
            f"ip:{client_host}",
            "auth.register",
            limit=resolved_settings.auth_rate_limit_per_minute,
        )
        try:
            user = await store.register_user(payload.email, payload.password)
        except (StoreConflictError, ValueError) as exc:
            code = status.HTTP_409_CONFLICT if isinstance(exc, StoreConflictError) else 422
            raise HTTPException(code, str(exc)) from exc
        token = new_session_token()
        await store.create_auth_session(
            user.id, token, ttl_seconds=resolved_settings.session_ttl_seconds
        )
        set_session_cookie(response, token)
        return user

    @app.post("/v1/auth/login", response_model=UserOut)
    async def login(
        payload: CredentialsIn, response: Response, request: Request
    ) -> User:
        client_host = request.client.host if request.client is not None else "unknown"
        await consume_or_reject(
            f"ip:{client_host}",
            "auth.login",
            limit=resolved_settings.auth_rate_limit_per_minute,
        )
        user = await store.verify_user(payload.email, payload.password)
        if user is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")
        token = new_session_token()
        await store.create_auth_session(
            user.id, token, ttl_seconds=resolved_settings.session_ttl_seconds
        )
        set_session_cookie(response, token)
        return user

    @app.post("/v1/auth/logout", status_code=204)
    async def logout(
        response: Response,
        token: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
    ) -> None:
        await store.revoke_auth_session(token or "")
        response.delete_cookie(SESSION_COOKIE, path="/")

    @app.get("/v1/me", response_model=UserOut)
    async def me(user: Annotated[User, Depends(current_user)]) -> User:
        return user

    @app.post("/v1/conversations", response_model=ConversationOut, status_code=201)
    async def create_conversation(
        payload: ConversationCreate,
        user: Annotated[User, Depends(current_user)],
    ):
        return await store.create_conversation(user.id, payload.title)

    @app.get("/v1/conversations", response_model=list[ConversationOut])
    async def list_conversations(
        user: Annotated[User, Depends(current_user)], limit: int = 50
    ):
        return await store.list_conversations(user.id, limit=limit)

    @app.get("/v1/conversations/{conversation_id}", response_model=ConversationOut)
    async def get_conversation(
        conversation_id: UUID,
        user: Annotated[User, Depends(current_user)],
    ):
        try:
            return await store.get_conversation(user.id, conversation_id)
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found") from exc

    @app.get(
        "/v1/conversations/{conversation_id}/messages",
        response_model=list[MessageOut],
    )
    async def list_messages(
        conversation_id: UUID,
        user: Annotated[User, Depends(current_user)],
        before_seq: int | None = None,
        limit: int = 100,
    ):
        try:
            return await store.list_messages(
                user.id, conversation_id, before_seq=before_seq, limit=limit
            )
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found") from exc

    @app.delete("/v1/conversations/{conversation_id}", status_code=204)
    async def delete_conversation(
        conversation_id: UUID,
        user: Annotated[User, Depends(current_user)],
    ) -> None:
        try:
            await store.delete_conversation(user.id, conversation_id)
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found") from exc
        except StoreConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc

    @app.delete("/v1/me", status_code=204)
    async def delete_account(
        user: Annotated[User, Depends(current_user)], response: Response
    ) -> None:
        try:
            await store.delete_user(user.id)
        except StoreConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        response.delete_cookie(SESSION_COOKIE, path="/")

    @app.get(
        "/v1/conversations/{conversation_id}/automation",
        response_model=AutomationOut | None,
    )
    async def get_automation(
        conversation_id: UUID,
        user: Annotated[User, Depends(current_user)],
    ):
        try:
            return await store.get_automation(user.id, conversation_id)
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found") from exc

    @app.put(
        "/v1/conversations/{conversation_id}/automation",
        response_model=AutomationOut,
    )
    async def configure_automation(
        conversation_id: UUID,
        payload: AutomationConfigIn,
        user: Annotated[User, Depends(current_user)],
    ):
        try:
            return await store.configure_automation(
                user.id,
                conversation_id,
                proactive_enabled=payload.proactive_enabled,
                drift_enabled=payload.drift_enabled,
                proactive_context=payload.proactive_context,
            )
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found") from exc

    @app.post(
        "/v1/conversations/{conversation_id}/proactive-events",
        response_model=ProactiveEventAccepted,
        status_code=202,
    )
    async def ingest_proactive_event(
        conversation_id: UUID,
        payload: ProactiveEventIn,
        user: Annotated[User, Depends(current_user)],
    ) -> ProactiveEventAccepted:
        await consume_or_reject(
            f"user:{user.id}",
            "proactive.ingest",
            limit=resolved_settings.message_rate_limit_per_minute,
        )
        try:
            item_id, accepted = await store.ingest_proactive_event(
                user.id,
                conversation_id,
                kind=payload.kind,
                source_id=payload.source_id,
                event_id=payload.event_id,
                payload=payload.payload,
            )
        except StoreStateError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        return ProactiveEventAccepted(item_id=item_id, accepted=accepted)

    @app.post(
        "/v1/conversations/{conversation_id}/schedules",
        response_model=ScheduleOut,
        status_code=201,
    )
    async def create_schedule(
        conversation_id: UUID,
        payload: ScheduleCreate,
        user: Annotated[User, Depends(current_user)],
    ):
        try:
            return await create_cloud_schedule(
                store,
                user.id,
                conversation_id,
                **payload.model_dump(),
            )
        except StoreNotFoundError as exc:
            raise HTTPException(404, "conversation not found") from exc
        except StoreConflictError as exc:
            raise HTTPException(409, str(exc)) from exc
        except (ValueError, OverflowError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/v1/schedules", response_model=list[ScheduleOut])
    async def list_schedules(
        user: Annotated[User, Depends(current_user)],
        include_finished: bool = False,
    ):
        return await store.list_scheduled_jobs(
            user.id, include_finished=include_finished
        )

    @app.delete("/v1/schedules/{job_id}", response_model=ScheduleOut)
    async def cancel_schedule(
        job_id: str, user: Annotated[User, Depends(current_user)]
    ):
        try:
            return await store.cancel_scheduled_job(user.id, job_id)
        except StoreNotFoundError as exc:
            raise HTTPException(404, "schedule not found") from exc
        except StoreStateError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post(
        "/v1/conversations/{conversation_id}/messages",
        response_model=RunAccepted,
        status_code=202,
    )
    async def submit_message(
        conversation_id: UUID,
        payload: MessageCreate,
        user: Annotated[User, Depends(current_user)],
        idempotency_key: Annotated[
            str | None, Header(alias="Idempotency-Key")
        ] = None,
    ) -> RunAccepted:
        await consume_or_reject(
            f"user:{user.id}",
            "message.submit",
            limit=resolved_settings.message_rate_limit_per_minute,
        )
        try:
            message, run = await store.append_user_message_and_run(
                user.id,
                conversation_id,
                payload.content,
                idempotency_key=idempotency_key,
                file_ids=payload.file_ids,
            )
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found") from exc
        except ValueError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
        except StoreConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        return RunAccepted(message_id=message.id, run_id=run.id)

    @app.post(
        "/v1/conversations/{conversation_id}/files",
        response_model=FileOut,
        status_code=201,
    )
    async def upload_file(
        conversation_id: UUID,
        payload: FileUploadIn,
        user: Annotated[User, Depends(current_user)],
    ):
        if sandbox is None:
            raise HTTPException(503, "isolated workspace service unavailable")
        try:
            content = base64.b64decode(payload.content_base64, validate=True)
        except ValueError as exc:
            raise HTTPException(422, "invalid base64 file content") from exc
        if not content or len(content) > 16_000_000:
            raise HTTPException(413, "file must be between 1 byte and 16 MB")
        filename = re.sub(r"[^A-Za-z0-9._ -]+", "_", Path(payload.filename).name).strip()
        if not filename or filename in {".", ".."}:
            raise HTTPException(422, "invalid filename")
        file_id = uuid4()
        workspace_path = f"uploads/{conversation_id}/{file_id.hex}/{filename}"
        try:
            await sandbox.write_binary(str(conversation_id), workspace_path, content)
            return await store.create_user_file(
                user.id,
                conversation_id,
                workspace_path=workspace_path,
                filename=filename,
                content_type=payload.content_type,
                size_bytes=len(content),
                sha256_hex=hashlib.sha256(content).hexdigest(),
            )
        except StoreNotFoundError as exc:
            raise HTTPException(404, "conversation not found") from exc

    @app.get("/v1/files/{file_id}")
    async def download_file(
        file_id: UUID, user: Annotated[User, Depends(current_user)]
    ) -> Response:
        if sandbox is None:
            raise HTTPException(503, "isolated workspace service unavailable")
        try:
            item = await store.get_user_file(user.id, file_id)
        except StoreNotFoundError as exc:
            raise HTTPException(404, "file not found") from exc
        content = await sandbox.read_binary(
            str(item.conversation_id), item.workspace_path
        )
        return Response(
            content,
            media_type=item.content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{item.filename.replace(chr(34), "_")}"',
                "X-Content-SHA256": item.sha256,
            },
        )

    @app.post(
        "/v1/conversations/{conversation_id}/channel-pairings",
        response_model=ChannelPairingOut,
        status_code=201,
    )
    async def create_channel_pairing(
        conversation_id: UUID,
        payload: ChannelPairingIn,
        user: Annotated[User, Depends(current_user)],
    ) -> ChannelPairingOut:
        code = secrets.token_urlsafe(18)
        try:
            pairing = await store.create_channel_pairing(
                user.id, conversation_id, payload.provider, code
            )
        except StoreNotFoundError as exc:
            raise HTTPException(404, "conversation not found") from exc
        return ChannelPairingOut(
            provider=payload.provider, code=code, expires_at=pairing.expires_at
        )

    @app.get("/v1/channel-links", response_model=list[ChannelLinkOut])
    async def list_channel_links(
        user: Annotated[User, Depends(current_user)],
    ):
        return await store.list_channel_links(user.id)

    @app.delete("/v1/channel-links/{link_id}", status_code=204)
    async def delete_channel_link(
        link_id: UUID, user: Annotated[User, Depends(current_user)]
    ) -> None:
        try:
            await store.delete_channel_link(user.id, link_id)
        except StoreNotFoundError as exc:
            raise HTTPException(404, "channel link not found") from exc

    @app.post("/v1/mcp-servers", response_model=McpServerOut, status_code=201)
    async def create_mcp_server(
        payload: McpServerCreate,
        user: Annotated[User, Depends(current_user)],
    ):
        if len(payload.headers) > 20 or any(
            len(key) > 200 or len(value) > 8_000
            for key, value in payload.headers.items()
        ):
            raise HTTPException(422, "MCP headers exceed the credential limit")
        try:
            base_url = validate_remote_mcp_url(payload.base_url)
            encrypted = CredentialVault().encrypt_json(payload.headers)
            return await store.create_mcp_server(
                user.id,
                name=payload.name,
                base_url=base_url,
                encrypted_headers=encrypted,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except StoreConflictError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/v1/mcp-servers", response_model=list[McpServerOut])
    async def list_mcp_servers(user: Annotated[User, Depends(current_user)]):
        return await store.list_mcp_servers(user.id)

    @app.delete("/v1/mcp-servers/{server_id}", status_code=204)
    async def delete_mcp_server(
        server_id: UUID, user: Annotated[User, Depends(current_user)]
    ) -> None:
        try:
            await store.delete_mcp_server(user.id, server_id)
        except StoreNotFoundError as exc:
            raise HTTPException(404, "MCP server not found") from exc

    @app.post("/v1/plugins", response_model=PluginOut, status_code=201)
    async def create_plugin(
        payload: PluginCreate,
        user: Annotated[User, Depends(current_user)],
    ):
        if len(payload.headers) > 20 or any(
            len(key) > 200 or len(value) > 8_000
            for key, value in payload.headers.items()
        ):
            raise HTTPException(422, "plugin headers exceed the credential limit")
        try:
            base_url = validate_remote_plugin_url(payload.base_url)
            async with httpx.AsyncClient(
                base_url=base_url.rstrip("/") + "/",
                headers=payload.headers,
                timeout=10,
                follow_redirects=False,
            ) as client:
                response = await client.get("v1/manifest")
                response.raise_for_status()
                manifest = validate_plugin_manifest(response.json())
            encrypted = CredentialVault().encrypt_json(payload.headers)
            return await store.create_plugin(
                user.id,
                name=payload.name,
                base_url=base_url,
                encrypted_headers=encrypted,
                manifest=manifest,
            )
        except (ValueError, httpx.HTTPError, json.JSONDecodeError) as exc:
            raise HTTPException(422, f"plugin service validation failed: {exc}") from exc
        except StoreConflictError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/v1/plugins", response_model=list[PluginOut])
    async def list_plugins(user: Annotated[User, Depends(current_user)]):
        return await store.list_plugins(user.id)

    @app.delete("/v1/plugins/{plugin_id}", status_code=204)
    async def delete_plugin(
        plugin_id: UUID, user: Annotated[User, Depends(current_user)]
    ) -> None:
        try:
            await store.delete_plugin(user.id, plugin_id)
        except StoreNotFoundError as exc:
            raise HTTPException(404, "plugin not found") from exc

    @app.get("/v1/subagents", response_model=list[SubagentJobOut])
    async def list_subagents(user: Annotated[User, Depends(current_user)]):
        return await store.list_subagent_jobs(user.id)

    @app.post("/v1/subagents/{task_id}/cancel", response_model=SubagentJobOut)
    async def cancel_subagent(
        task_id: str, user: Annotated[User, Depends(current_user)]
    ):
        try:
            return await store.cancel_subagent_job(user.id, task_id)
        except StoreNotFoundError as exc:
            raise HTTPException(404, "subagent job not found") from exc

    @app.post("/v1/skills", response_model=SkillOut, status_code=201)
    async def create_skill(
        payload: SkillCreate, user: Annotated[User, Depends(current_user)]
    ):
        try:
            parsed = parse_skill_document(payload.content)
            return await store.create_skill(user.id, **parsed)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except StoreConflictError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/v1/skills", response_model=list[SkillOut])
    async def list_skills(user: Annotated[User, Depends(current_user)]):
        return await store.list_skills(user.id)

    @app.delete("/v1/skills/{skill_id}", status_code=204)
    async def delete_skill(
        skill_id: UUID, user: Annotated[User, Depends(current_user)]
    ) -> None:
        try:
            await store.delete_skill(user.id, skill_id)
        except StoreNotFoundError as exc:
            raise HTTPException(404, "skill not found") from exc

    @app.get("/v1/runs/{run_id}", response_model=RunOut)
    async def get_run(
        run_id: UUID, user: Annotated[User, Depends(current_user)]
    ):
        try:
            return await store.get_run(user.id, run_id)
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found") from exc

    @app.post("/v1/runs/{run_id}/cancel", response_model=RunOut)
    async def cancel_run(
        run_id: UUID, user: Annotated[User, Depends(current_user)]
    ):
        try:
            return await store.request_cancel(user.id, run_id)
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found") from exc

    @app.get("/v1/runs/{run_id}/events", response_model=list[RunEventOut])
    async def list_run_events(
        run_id: UUID,
        user: Annotated[User, Depends(current_user)],
        after: int = 0,
    ):
        try:
            return await store.list_run_events(user.id, run_id, after_seq=after)
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found") from exc

    @app.get("/v1/runs/{run_id}/events/stream")
    async def stream_run_events(
        run_id: UUID,
        request: Request,
        user: Annotated[User, Depends(current_user)],
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        try:
            run = await store.get_run(user.id, run_id)
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found") from exc
        try:
            initial_seq = max(0, int(last_event_id or "0"))
        except ValueError as exc:
            raise HTTPException(422, "Last-Event-ID must be an integer") from exc

        async def event_stream() -> AsyncIterator[str]:
            cursor = initial_seq
            terminal = {"completed", "failed", "cancelled"}
            while True:
                if await request.is_disconnected():
                    return
                events = await store.list_run_events(
                    user.id, run_id, after_seq=cursor
                )
                for event in events:
                    cursor = event.seq
                    payload = json.dumps(
                        event.data, ensure_ascii=False, separators=(",", ":")
                    )
                    yield (
                        f"id: {event.seq}\n"
                        f"event: {event.event_type}\n"
                        f"data: {payload}\n\n"
                    )
                current = await store.get_run(user.id, run_id)
                if current.status in terminal and not events:
                    return
                await asyncio.sleep(
                    max(0.05, resolved_settings.sse_poll_interval_seconds)
                )

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
        )

    return app
