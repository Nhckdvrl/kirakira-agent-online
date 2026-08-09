"""FastAPI surface for the durable multi-user Cloud application."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import json
import logging
import time
from typing import Annotated
from uuid import UUID
from uuid import uuid4

from fastapi import Cookie, Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse
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
    UserOut,
)
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

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if engine is not None:
            await engine.dispose()

    app = FastAPI(title="Kirakira Cloud API", version="0.1.0", lifespan=lifespan)
    app.state.store = store

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
            "Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'"
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
            )
        except StoreNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "conversation not found") from exc
        except ValueError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
        except StoreConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        return RunAccepted(message_id=message.id, run_id=run.id)

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
