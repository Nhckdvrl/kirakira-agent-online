from __future__ import annotations

import os
import uvicorn


def main() -> None:
    uvicorn.run(
        "cloud.channel_gateway:create_app",
        factory=True,
        host=os.getenv("KIRAKIRA_CHANNEL_HOST", "127.0.0.1"),
        port=int(os.getenv("KIRAKIRA_CHANNEL_PORT", "8020")),
        workers=1,
        proxy_headers=False,
    )


if __name__ == "__main__":
    main()
