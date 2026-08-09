"""Cloud API process entrypoint."""

from __future__ import annotations

import os
import uvicorn

from cloud.logging import configure_cloud_logging


def main() -> None:
    configure_cloud_logging()
    uvicorn.run(
        "cloud.api:create_app",
        factory=True,
        host=os.getenv("KIRAKIRA_API_HOST", "0.0.0.0"),
        port=int(os.getenv("KIRAKIRA_API_PORT", "8000")),
        workers=max(1, int(os.getenv("KIRAKIRA_API_WORKERS", "1"))),
        proxy_headers=True,
        forwarded_allow_ips=os.getenv("KIRAKIRA_FORWARDED_ALLOW_IPS", "127.0.0.1"),
    )


if __name__ == "__main__":
    main()
