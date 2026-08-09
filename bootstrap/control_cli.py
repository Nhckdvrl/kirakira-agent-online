"""``main.py control ...`` —— 控制面的命令行客户端。

它不启动 agent,而是连上**已经在跑的** agent 的 workspace 私有 socket。
用途:在不打断渠道会话的前提下观测状态、列会话、跑一轮 programmatic turn、
中断在途 turn、排空插件。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from agent.control.client import ControlClient, RemoteControlError

USAGE = """用法: main.py control <子命令> [参数]

  status                     打印 runtime 就绪状态与 boot id
  threads [--limit N]        列出会话(thread)
  read <threadId> [--turns]  读取一个 thread,可选带上历史 turn
  ask <threadId> <文本>      在指定 thread 上跑一轮,流式打印增量
  new [--ask 文本]           新建 programmatic thread,可选立即问一句
  interrupt <threadId> <turnId>
  consolidate <threadId>     强制归档一个 thread 的记忆
  plugin-drain <pluginId>    停用插件并等待其代际排空

环境变量:
  KIRAKIRA_CONTROL_ENDPOINT  覆盖 socket 路径(默认 <workspace>/.kirakira/control.sock)
  KIRAKIRA_CONTROL_TOKEN     workspace token(服务端配置了才需要)
"""


def _endpoint(workspace: Path) -> str:
    override = os.getenv("KIRAKIRA_CONTROL_ENDPOINT", "").strip()
    if override:
        return override
    return str(workspace / ".kirakira" / "control.sock")


def _flag(args: list[str], name: str, default: str = "") -> str:
    if name in args:
        index = args.index(name)
        if index + 1 < len(args):
            return args[index + 1]
    return default


async def _run(args: list[str], workspace: Path) -> int:
    if not args or args[0] in {"-h", "--help", "help"}:
        print(USAGE)
        return 0
    action, rest = args[0], args[1:]
    endpoint = _endpoint(workspace)
    if not endpoint.count(":") == 1 and not Path(endpoint).exists():
        print(
            f"找不到控制面 socket: {endpoint}\n"
            "agent 没在跑,或 workspace 指错了。先用 `uv run python main.py` 启动。",
            file=sys.stderr,
        )
        return 2

    try:
        client = await ControlClient.connect(
            endpoint,
            workspace_token=os.getenv("KIRAKIRA_CONTROL_TOKEN", "").strip() or None,
        )
    except (ConnectionError, OSError) as exc:
        print(f"连接控制面失败: {exc}", file=sys.stderr)
        return 2

    try:
        return await _dispatch(client, action, rest)
    except RemoteControlError as exc:
        print(f"控制面返回错误 [{exc.code}]: {exc}", file=sys.stderr)
        return 1
    finally:
        await client.close()


async def _dispatch(client: ControlClient, action: str, rest: list[str]) -> int:
    def show(payload: object) -> int:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if action == "status":
        return show(await client.request("server/status", {}))
    if action == "threads":
        limit = int(_flag(rest, "--limit", "50"))
        return show(await client.request("thread/list", {"cursor": None, "limit": limit}))
    if action == "read":
        if not rest:
            print("read 需要 threadId", file=sys.stderr)
            return 2
        return show(
            await client.request(
                "thread/read", {"threadId": rest[0], "includeTurns": "--turns" in rest}
            )
        )
    if action == "consolidate":
        if not rest:
            print("consolidate 需要 threadId", file=sys.stderr)
            return 2
        return show(await client.request("thread/consolidate/start", {"threadId": rest[0]}))
    if action == "interrupt":
        if len(rest) < 2:
            print("interrupt 需要 threadId turnId", file=sys.stderr)
            return 2
        return show(
            await client.request("turn/interrupt", {"threadId": rest[0], "turnId": rest[1]})
        )
    if action == "plugin-drain":
        if not rest:
            print("plugin-drain 需要 pluginId", file=sys.stderr)
            return 2
        return show(await client.request("plugin/disable-and-drain", {"pluginId": rest[0]}))
    if action == "new":
        thread = await client.start_thread({"source": "control-cli"})
        thread_id = str(thread["id"])
        print(f"thread: {thread_id}")
        question = _flag(rest, "--ask")
        if not question:
            return 0
        return await _ask(client, thread_id, question)
    if action == "ask":
        if len(rest) < 2:
            print("ask 需要 threadId 与文本", file=sys.stderr)
            return 2
        return await _ask(client, rest[0], " ".join(rest[1:]))

    print(f"未知子命令: {action}\n\n{USAGE}", file=sys.stderr)
    return 2


async def _ask(client: ControlClient, thread_id: str, text: str) -> int:
    """跑一轮并按事件流实时打印:工具调用可见,回复逐块吐出。"""
    handle = await client.start_turn(thread_id, text)
    async for event in handle.events():
        method = event.get("method")
        params = event.get("params") or {}
        if method == "item/started":
            item = params.get("item") or {}
            if item.get("type") == "toolCall":
                data = item.get("data") or {}
                print(f"  [tool] {data.get('name', '')}", file=sys.stderr)
        elif method == "item/assistantMessage/delta":
            sys.stdout.write(str(params.get("delta") or ""))
            sys.stdout.flush()
        elif method == "turn/completed":
            turn = params.get("turn") or {}
            print()
            status = turn.get("status")
            if status != "completed":
                error = turn.get("error") or {}
                print(
                    f"[{status}] {error.get('type', '')}: {error.get('message', '')}",
                    file=sys.stderr,
                )
                return 1
            if (turn.get("usage") or {}).get("requestCount"):
                print(
                    f"  ({turn['usage']['requestCount']} 次模型请求, "
                    f"{turn.get('durationMs')} ms)",
                    file=sys.stderr,
                )
    return 0


def main(args: list[str], workspace: Path) -> int:
    return asyncio.run(_run(args, workspace))
