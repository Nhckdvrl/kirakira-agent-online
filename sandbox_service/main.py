"""Sandbox service process entrypoint."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    uvicorn.run(
        "sandbox_service.api:create_app",
        factory=True,
        host=os.getenv("KIRAKIRA_SANDBOX_HOST", "127.0.0.1"),
        port=int(os.getenv("KIRAKIRA_SANDBOX_PORT", "8010")),
        workers=1,
        proxy_headers=False,
    )


if __name__ == "__main__":
    main()

