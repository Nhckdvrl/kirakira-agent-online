"""Cloud database configuration and async session lifecycle."""

from __future__ import annotations

import os
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy import Engine, create_engine


def async_database_url(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return url
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url.removeprefix("postgresql://")
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url.removeprefix("postgres://")
    return url


def sync_database_url(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return url
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url.removeprefix("postgresql://")
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url.removeprefix("postgres://")
    if url.startswith("sqlite+aiosqlite://"):
        return "sqlite+pysqlite://" + url.removeprefix("sqlite+aiosqlite://")
    return url


@dataclass(frozen=True)
class CloudSettings:
    database_url: str
    session_cookie_secure: bool = True
    session_ttl_seconds: int = 60 * 60 * 24 * 14
    allowed_origins: tuple[str, ...] = ()
    auth_rate_limit_per_minute: int = 10
    message_rate_limit_per_minute: int = 30
    sse_poll_interval_seconds: float = 0.5

    @classmethod
    def from_env(cls) -> CloudSettings:
        url = os.getenv("KIRAKIRA_DATABASE_URL", "").strip()
        if not url:
            raise RuntimeError("KIRAKIRA_DATABASE_URL is required for Cloud mode")
        secure = os.getenv("KIRAKIRA_SESSION_COOKIE_SECURE", "true").lower()
        allowed_origins = tuple(
            item.strip().rstrip("/")
            for item in os.getenv("KIRAKIRA_ALLOWED_ORIGINS", "").split(",")
            if item.strip()
        )
        if secure not in {"0", "false", "no", "off"} and not allowed_origins:
            raise RuntimeError(
                "KIRAKIRA_ALLOWED_ORIGINS is required with secure Cloud cookies"
            )
        return cls(
            database_url=async_database_url(url),
            session_cookie_secure=secure not in {"0", "false", "no", "off"},
            session_ttl_seconds=int(
                os.getenv("KIRAKIRA_SESSION_TTL_SECONDS", str(60 * 60 * 24 * 14))
            ),
            allowed_origins=allowed_origins,
            auth_rate_limit_per_minute=int(
                os.getenv("KIRAKIRA_AUTH_RATE_LIMIT_PER_MINUTE", "10")
            ),
            message_rate_limit_per_minute=int(
                os.getenv("KIRAKIRA_MESSAGE_RATE_LIMIT_PER_MINUTE", "30")
            ),
            sse_poll_interval_seconds=float(
                os.getenv("KIRAKIRA_SSE_POLL_INTERVAL_SECONDS", "0.5")
            ),
        )


def build_engine(settings: CloudSettings, **kwargs: object) -> AsyncEngine:
    return create_async_engine(async_database_url(settings.database_url), **kwargs)


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


def build_sync_engine(settings: CloudSettings, **kwargs: object) -> Engine:
    return create_engine(sync_database_url(settings.database_url), **kwargs)
