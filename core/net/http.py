"""Shared asynchronous HTTP resources used by runtime integrations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RequestBudget:
    """Request budget carried across an integration call."""

    total_timeout_s: float = 40.0


class _HttpxResponse:
    def __init__(self, response: Any) -> None:
        self._response = response

    def raise_for_status(self) -> None:
        self._response.raise_for_status()

    def json(self) -> Any:
        return self._response.json()

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def text(self) -> str:
        return self._response.text

    @property
    def content(self) -> bytes:
        return self._response.content

    @property
    def headers(self) -> Any:
        return self._response.headers

    @property
    def url(self) -> Any:
        return self._response.url

    @property
    def encoding(self) -> Any:
        return self._response.encoding


class HttpRequester:
    """Small httpx-backed requester shared by external integrations."""

    def __init__(self, name: str = "external_default") -> None:
        self._name = name

    async def post(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json: Any = None,
        timeout_s: float = 30.0,
        budget: RequestBudget | None = None,
        **_ignore: Any,
    ) -> _HttpxResponse:
        import httpx

        timeout = budget.total_timeout_s if budget else timeout_s
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, headers=headers, json=json)
        return _HttpxResponse(resp)

    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        follow_redirects: bool = True,
        timeout_s: float = 30.0,
        budget: RequestBudget | None = None,
        **_ignore: Any,
    ) -> _HttpxResponse:
        import httpx

        timeout = budget.total_timeout_s if budget else timeout_s
        async with httpx.AsyncClient(
            timeout=timeout, follow_redirects=follow_redirects
        ) as client:
            resp = await client.get(url, headers=headers)
        return _HttpxResponse(resp)


class SharedHttpResources:
    """Runtime owner for shared outbound HTTP clients."""

    def __init__(self) -> None:
        self.external_default = get_default_http_requester("external_default")

    async def aclose(self) -> None:
        return None


_DEFAULT_REQUESTER: HttpRequester | None = None


def get_default_http_requester(name: str = "external_default") -> HttpRequester:
    global _DEFAULT_REQUESTER
    if _DEFAULT_REQUESTER is None:
        _DEFAULT_REQUESTER = HttpRequester(name)
    return _DEFAULT_REQUESTER
