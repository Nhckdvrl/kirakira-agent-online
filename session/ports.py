"""Persistence ports consumed by the agent runtime.

Local mode supplies ``SessionManager``. Cloud mode can supply a PostgreSQL-backed
adapter without making the reasoning pipeline depend on either database.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class TranscriptStore(Protocol):
    def get_or_create(self, key: str) -> Any: ...

    def session_exists(self, key: str) -> bool: ...

    def peek_next_message_id(self, session_key: str) -> str: ...

    def save(self, session: Any) -> None: ...

    async def save_async(self, session: Any) -> None: ...
