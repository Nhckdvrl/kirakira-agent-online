"""Cloud worker process entrypoint."""

from __future__ import annotations

import asyncio
import signal

from cloud.runtime import build_cloud_worker_runtime_from_env
from cloud.logging import configure_cloud_logging


async def run() -> None:
    runtime = await build_cloud_worker_runtime_from_env()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                signum,
                lambda: (runtime.worker.stop(), runtime.automation_worker.stop()),
            )
        except NotImplementedError:  # pragma: no cover - Windows event loop
            pass
    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(runtime.worker.run_forever(), name="passive-runs")
            tasks.create_task(
                runtime.automation_worker.run_forever(), name="agent-automations"
            )
    finally:
        await runtime.aclose()


def main() -> None:
    configure_cloud_logging()
    asyncio.run(run())


if __name__ == "__main__":
    main()
