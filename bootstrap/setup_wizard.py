"""Reference-aligned first-run setup and workspace initialization."""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import click


DEFAULT_WORKSPACE = "~/.kirakira/workspace"


@dataclass
class InitSummary:
    created: list[Path] = field(default_factory=list)
    overwritten: list[Path] = field(default_factory=list)
    skipped: list[Path] = field(default_factory=list)


@dataclass
class WizardAnswers:
    workspace: Path
    model: str = ""
    base_url: str = ""
    api_key: str = ""
    context_window: int = 128_000
    telegram_enabled: bool = False
    telegram_token: str = ""
    telegram_allow_from: list[str] = field(default_factory=list)
    proactive_enabled: bool = False
    proactive_channel: str = ""
    proactive_chat_id: str = ""
    qqbot_enabled: bool = False
    qqbot_app_id: str = ""
    qqbot_client_secret: str = ""
    qqbot_user_openid: str = ""
    qq_enabled: bool = False
    qq_bot_uin: str = ""
    qq_api_base_url: str = "http://127.0.0.1:3000"
    qq_allow_from: list[str] = field(default_factory=list)
    qq_groups: list[str] = field(default_factory=list)
    qq_access_token: str = ""
    drift_enabled: bool = False


def _toml_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _atomic_write(path: Path, content: str, *, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.setup-", dir=path.parent)
    temp = Path(raw_temp)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None:
            temp.chmod(mode)
        os.replace(temp, path)
    finally:
        if temp.exists():
            temp.unlink()


def _backup_existing(path: Path) -> None:
    if path.exists():
        shutil.copy2(path, path.with_name(f"{path.name}.before-setup.bak"))


def _render_config(a: WizardAnswers) -> str:
    telegram_enabled = str(a.telegram_enabled).lower()
    qq_enabled = str(a.qq_enabled).lower()
    proactive_enabled = str(a.proactive_enabled).lower()
    drift_enabled = str(a.drift_enabled).lower()
    allow_from = ", ".join(_toml_string(item) for item in a.telegram_allow_from)
    proactive_channel = a.proactive_channel if a.proactive_enabled else ""
    telegram_token = '"${TELEGRAM_BOT_TOKEN}"' if a.telegram_enabled else '""'
    qqbot_secret = '"${QQBOT_CLIENT_SECRET}"' if a.qqbot_enabled else '""'
    qqbot_allow = (
        _toml_string(a.qqbot_user_openid) if a.qqbot_user_openid else ""
    )
    qq_allow = ", ".join(_toml_string(item) for item in a.qq_allow_from)
    qq_group_tables = "\n".join(
        "\n".join(
            [
                "[[channels.qq.groups]]",
                f"group_id = {_toml_string(group_id)}",
                f"allow_from = [{qq_allow}]",
                "require_at = true",
            ]
        )
        for group_id in a.qq_groups
    )
    return f'''[runtime]
workspace = {_toml_string(str(a.workspace))}

[llm.main]
model = {_toml_string(a.model)}
api_key = "${{KIRAKIRA_MAIN_API_KEY}}"
base_url = {_toml_string(a.base_url)}
enable_thinking = false
context_window = {a.context_window}

[agent]
max_iterations = 40
system_prompt = "你是 Kirakira。使用与用户相同的语言回答；执行前核实，完成后给出可验证结果。"

[memory.embedding]
model = ""
api_key = ""
base_url = ""

[channels.chat]
enabled = true
host = "127.0.0.1"
port = 6322
channel_name = "web"

[channels.telegram]
enabled = {telegram_enabled}
token = {telegram_token}
allow_from = [{allow_from}]
channel_name = "telegram"

[channels.qq]
enabled = {qq_enabled}
bot_uin = {_toml_string(a.qq_bot_uin)}
api_base_url = {_toml_string(a.qq_api_base_url)}
access_token = {('"${ONEBOT_ACCESS_TOKEN}"' if a.qq_enabled and a.qq_access_token else '""')}
allow_from = [{qq_allow}]
require_at = true
channel_name = "qq"
{qq_group_tables}

[channels.qqbot]
enabled = {str(a.qqbot_enabled).lower()}
app_id = {_toml_string(a.qqbot_app_id)}
client_secret = {qqbot_secret}
allow_from = [{qqbot_allow}]
channel_name = "qqbot"
api_base_url = "https://api.sgroup.qq.com"

[proactive]
enabled = {proactive_enabled}
tick_interval_s1 = 2400
tick_interval_s0 = 4800
tick_jitter = 0.3
max_tokens = 1024
model = ""

[proactive.target]
channel = {_toml_string(proactive_channel)}
chat_id = {_toml_string(a.proactive_chat_id)}

[proactive.agent]
content_limit = 5
delivery_cooldown_hours = 1
content_max_age_days = 14

[proactive.drift]
enabled = {drift_enabled}
min_interval_hours = 3
max_steps = 20
'''


def _render_env(a: WizardAnswers) -> str:
    lines = [f"KIRAKIRA_MAIN_API_KEY={a.api_key}"]
    if a.telegram_enabled:
        lines.append(f"TELEGRAM_BOT_TOKEN={a.telegram_token}")
    if a.qqbot_enabled:
        lines.append(f"QQBOT_CLIENT_SECRET={a.qqbot_client_secret}")
    if a.qq_enabled and a.qq_access_token:
        lines.append(f"ONEBOT_ACCESS_TOKEN={a.qq_access_token}")
    return "\n".join(lines) + "\n"


def _validate_telegram_token(token: str) -> str | None:
    """Validate the token up front, matching Reference's setup failure boundary."""

    try:
        import httpx

        response = httpx.get(f"https://api.telegram.org/bot{token}/getMe", timeout=8)
        payload = response.json()
        if payload.get("ok"):
            username = payload.get("result", {}).get("username", "")
            click.echo(click.style(f"  ✓ bot 验证成功：@{username}", fg="green"))
            return None
        return f"token 无效（{payload.get('description', response.status_code)}）"
    except Exception as exc:
        return f"网络错误：{exc}"


def _normalize_telegram_identity(value: str) -> str:
    identity = value.strip().removeprefix("@")
    if identity.isdigit():
        return identity
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", identity):
        return identity
    raise click.BadParameter(
        "请填写数字 user id 或 Telegram username（不是带空格的显示名称）"
    )


def _validate_telegram_chat_target(
    token: str, chat_id: str, expected_identity: str = ""
) -> str | None:
    if not chat_id.strip().lstrip("-").isdigit():
        return "chat_id 必须是数字；username 只能填在上一步白名单中"
    try:
        import httpx

        base = f"https://api.telegram.org/bot{token}"
        with httpx.Client(timeout=8) as client:
            bot = client.get(base + "/getMe").json().get("result") or {}
            chat_payload = client.get(
                base + "/getChat", params={"chat_id": chat_id}
            ).json()
        if not chat_payload.get("ok"):
            return "Telegram 找不到这个 chat_id"
        chat = chat_payload.get("result") or {}
        if str(chat.get("id") or "") == str(bot.get("id") or ""):
            return "chat_id 是机器人自己的 ID，不是用户 ID"
        if str(chat.get("type") or "") != "private":
            return "当前向导只接受用户私聊 chat_id"
        expected = expected_identity.removeprefix("@").lower().strip()
        actual_id = str(chat.get("id") or "").lower()
        actual_username = str(chat.get("username") or "").lower()
        if expected and expected not in {actual_id, actual_username}:
            return "chat_id 对应的用户与上一步填写的 user id/username 不一致"
        return None
    except Exception as exc:
        return f"chat_id 验证失败：{exc}"


def _prompt_telegram_chat_id(token: str, expected_identity: str) -> str:
    while True:
        chat_id = click.prompt("未自动获取到，请手动填写 chat_id").strip()
        error = _validate_telegram_chat_target(token, chat_id, expected_identity)
        if error is None:
            return chat_id
        click.echo(click.style(f"  ✗ {error}", fg="red"))


def _fetch_telegram_chat_id(token: str, username_or_id: str, timeout_s: int = 60) -> str | None:
    """Consume a fresh Telegram update and resolve the proactive target chat id."""

    import httpx

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    wanted = username_or_id.removeprefix("@").lower()
    try:
        with httpx.Client(timeout=12) as client:
            last = client.get(url, params={"offset": -1, "limit": 1}).json().get("result", [])
            offset = int(last[-1]["update_id"]) + 1 if last else 0
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                payload = client.get(
                    url,
                    params={"offset": offset, "timeout": min(10, max(1, int(deadline - time.time())))},
                ).json()
                for update in payload.get("result", []):
                    offset = int(update["update_id"]) + 1
                    message = update.get("message") or update.get("channel_post") or {}
                    sender = message.get("from") or {}
                    chat = message.get("chat") or {}
                    username = str(sender.get("username") or "").lower()
                    user_id = str(sender.get("id") or "").lower()
                    chat_id = str(chat.get("id") or "")
                    is_private_user = (
                        str(chat.get("type") or "") == "private"
                        and chat_id == user_id
                    )
                    if is_private_user and wanted in {username, user_id}:
                        try:
                            client.get(
                                url,
                                params={"offset": offset, "limit": 1, "timeout": 0},
                            )
                        except Exception as exc:
                            click.echo(
                                click.style(
                                    f"  ! chat_id 已获取，但确认 Telegram update 失败：{exc}",
                                    fg="yellow",
                                )
                            )
                        return chat_id or None
    except Exception as exc:
        click.echo(click.style(f"  ✗ 获取 chat_id 失败：{exc}", fg="red"))
    return None


def _fetch_telegram_chat_id_with_spinner(token: str, username_or_id: str) -> str | None:
    result: list[str | None] = [None]
    done = threading.Event()

    def poll() -> None:
        result[0] = _fetch_telegram_chat_id(token, username_or_id)
        done.set()

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    index = 0
    while not done.wait(timeout=0.1):
        click.echo(f"\r  {frames[index % len(frames)]} 等待消息中...", nl=False)
        index += 1
    click.echo("\r" + " " * 30 + "\r", nl=False)
    thread.join()
    return result[0]


def _validate_qqbot_credentials(app_id: str, client_secret: str) -> str | None:
    try:
        import httpx

        response = httpx.post(
            "https://bots.qq.com/app/getAppAccessToken",
            json={"appId": app_id, "clientSecret": client_secret},
            timeout=10,
        )
        payload = response.json()
        if payload.get("access_token"):
            click.echo(click.style("  ✓ AppID / AppSecret 验证成功", fg="green"))
            return None
        return f"token 获取失败（{payload}）"
    except Exception as exc:
        return f"网络错误：{exc}"


def _validate_onebot_api(api_base_url: str, access_token: str) -> str | None:
    try:
        import httpx

        headers = (
            {"Authorization": f"Bearer {access_token}"} if access_token else None
        )
        response = httpx.post(
            f"{api_base_url.rstrip('/')}/get_status",
            json={},
            headers=headers,
            timeout=8,
        )
        payload = response.json()
        if response.is_success and payload.get("retcode") in (None, 0):
            click.echo(click.style("  ✓ OneBot API 验证成功", fg="green"))
            return None
        return f"OneBot 返回异常（{payload}）"
    except Exception as exc:
        return f"连接失败：{exc}"


async def _async_fetch_qqbot_openid(
    app_id: str,
    client_secret: str,
    timeout_s: int,
    stop: threading.Event,
) -> str | None:
    """Reference setup protocol: token → gateway → identify → first C2C openid."""

    import httpx
    import websockets

    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            "https://bots.qq.com/app/getAppAccessToken",
            json={"appId": app_id, "clientSecret": client_secret},
        )
        token = str(response.json().get("access_token") or "")
        if not token:
            return None
        response = await client.get(
            "https://api.sgroup.qq.com/gateway",
            headers={"Authorization": f"QQBot {token}"},
        )
        gateway_url = str(response.json().get("url") or "")
        if not gateway_url:
            return None

    try:
        async with asyncio.timeout(timeout_s):
            async with websockets.connect(gateway_url) as websocket:
                async for raw in websocket:
                    if stop.is_set():
                        return None
                    payload = json.loads(raw)
                    if payload.get("op") == 10:
                        await websocket.send(
                            json.dumps(
                                {
                                    "op": 2,
                                    "d": {
                                        "token": f"QQBot {token}",
                                        "intents": 1 << 25,
                                        "shard": [0, 1],
                                    },
                                }
                            )
                        )
                    elif payload.get("op") == 0 and payload.get("t") == "C2C_MESSAGE_CREATE":
                        data = payload.get("d") or {}
                        author = data.get("author") or {}
                        openid = str(author.get("user_openid") or data.get("user_openid") or "")
                        if openid:
                            return openid
    except TimeoutError:
        return None
    return None


def _fetch_qqbot_openid_with_spinner(
    app_id: str, client_secret: str, timeout_s: int = 90
) -> str | None:
    result: list[str | None] = [None]
    done = threading.Event()

    def poll() -> None:
        try:
            result[0] = asyncio.run(
                _async_fetch_qqbot_openid(app_id, client_secret, timeout_s, done)
            )
        except Exception as exc:
            click.echo(click.style(f"  ✗ 获取 user_openid 失败：{exc}", fg="red"))
        done.set()

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    index = 0
    while not done.wait(timeout=0.1):
        click.echo(f"\r  {frames[index % len(frames)]} 等待消息中...", nl=False)
        index += 1
    click.echo("\r" + " " * 30 + "\r", nl=False)
    thread.join()
    return result[0]


def initialize_workspace(
    config_path: Path,
    workspace: Path,
    *,
    force: bool = False,
) -> InitSummary:
    """Create the same minimum runtime boundaries that Kirakira actually owns."""

    summary = InitSummary()
    template = Path(__file__).resolve().parent.parent / "config.example.toml"
    if config_path.exists() and not force:
        summary.skipped.append(config_path)
    else:
        existed = config_path.exists()
        content = template.read_text(encoding="utf-8")
        workspace_line = f"workspace = {_toml_string(str(workspace))}"
        if "[runtime]" in content:
            content = content.replace('workspace = "."', workspace_line, 1)
        else:
            # The public template is Cloud-only. This unpublished legacy adapter
            # still injects its local workspace so its algorithm regression
            # fixture remains self-contained.
            content = f"[runtime]\n{workspace_line}\n\n{content}"
        _atomic_write(config_path, content, mode=0o600)
        (summary.overwritten if existed else summary.created).append(config_path)

    directories = (
        "sessions",
        "memory",
        "skills",
        "plugins",
        "mcp/servers",
        "proactive/inbox",
        "drift/skills",
        ".kirakira/plugins",
        ".kirakira/plugin-data",
    )
    for relative in directories:
        path = workspace / relative
        existed = path.exists()
        path.mkdir(parents=True, exist_ok=True)
        (summary.skipped if existed else summary.created).append(path)

    from proactive_v2.loop import _PROACTIVE_CONTEXT_TEMPLATE
    from plugins.wake_proactive.sources import _INBOX_README
    from plugins.drift_flow.skills import ensure_example_skill

    files = {
        workspace / "PROACTIVE_CONTEXT.md": _PROACTIVE_CONTEXT_TEMPLATE,
        workspace / "proactive/inbox/README.md": _INBOX_README,
        workspace / ".kirakira/schedules.json": "[]\n",
    }
    for path, content in files.items():
        if path.exists() and not force:
            summary.skipped.append(path)
            continue
        existed = path.exists()
        _atomic_write(path, content)
        (summary.overwritten if existed else summary.created).append(path)
    ensure_example_skill(workspace)
    return summary


def run_setup_wizard(config_path: Path, workspace: Path) -> None:
    """Interactive setup patterned after ``Reference/bootstrap/setup_wizard.py``."""

    click.echo(click.style("\n══ kirakira 初始化向导 ══\n", bold=True))
    click.echo(click.style("  全程按回车使用括号内的默认值", dim=True))
    if config_path.exists():
        click.echo(f"\n已存在配置文件 {config_path}")
        if not click.confirm("覆盖并重新配置？", default=False):
            click.echo("已取消。")
            return

    click.echo(f"\n{click.style('[1/5]', bold=True)} 主模型\n")
    model = click.prompt("模型名", default="deepseek-chat")
    base_url = click.prompt("base_url（OpenAI 兼容格式）", default="https://api.deepseek.com/v1")
    api_key = click.prompt("API key", hide_input=True)
    context_window = click.prompt("上下文大小（tokens）", type=click.IntRange(min=1), default=128_000)

    click.echo(f"\n{click.style('[2/5]', bold=True)} Telegram + Proactive\n")
    telegram_enabled = click.confirm("配置 Telegram 频道？", default=True)
    telegram_token = ""
    telegram_allow_from: list[str] = []
    proactive_enabled = False
    proactive_channel = ""
    proactive_chat_id = ""
    drift_enabled = False
    if telegram_enabled:
        while True:
            telegram_token = click.prompt("Bot token", hide_input=True)
            token_error = _validate_telegram_token(telegram_token)
            if token_error is None:
                break
            click.echo(click.style(f"  ✗ {token_error}，请重新输入", fg="red"))
        while True:
            try:
                telegram_user = _normalize_telegram_identity(
                    click.prompt("允许使用的 Telegram user id 或用户名")
                )
                break
            except click.BadParameter as exc:
                click.echo(click.style(f"  ✗ {exc.format_message()}", fg="red"))
        telegram_allow_from = [telegram_user]
        proactive_enabled = click.confirm("开启 proactive 主动推送？", default=True)
        if proactive_enabled:
            proactive_channel = "telegram"
            click.echo("  现在向 bot 发任意一条消息，发完后回到这里继续。")
            click.pause(info="发完消息后按回车继续...")
            proactive_chat_id = _fetch_telegram_chat_id_with_spinner(
                telegram_token, telegram_user
            ) or _prompt_telegram_chat_id(telegram_token, telegram_user)
            drift_enabled = click.confirm("主动链路空转时开启 Drift？", default=False)

    click.echo(f"\n{click.style('[3/5]', bold=True)} 官方 QQBot（可跳过）\n")
    qqbot_enabled = click.confirm("配置腾讯开放平台官方 QQBot？", default=False)
    qqbot_app_id = ""
    qqbot_client_secret = ""
    qqbot_user_openid = ""
    if qqbot_enabled:
        qqbot_app_id = click.prompt("AppID")
        qqbot_client_secret = click.prompt("AppSecret (client_secret)", hide_input=True)
        credential_error = _validate_qqbot_credentials(
            qqbot_app_id, qqbot_client_secret
        )
        if credential_error:
            click.echo(click.style(f"  ! 凭据验证失败：{credential_error}", fg="yellow"))
        click.echo("  现在在 QQ 中向 bot 发任意一条私聊消息。")
        click.pause(info="发完消息后按回车继续...")
        qqbot_user_openid = _fetch_qqbot_openid_with_spinner(
            qqbot_app_id, qqbot_client_secret
        ) or click.prompt("未自动获取到，请手动填写 user_openid", default="", show_default=False)
        if qqbot_user_openid and not proactive_enabled:
            if click.confirm("开启 proactive 主动推送（via QQBot）？", default=True):
                proactive_enabled = True
                proactive_channel = "qqbot"
                proactive_chat_id = f"c2c:{qqbot_user_openid}"
                drift_enabled = click.confirm("主动链路空转时开启 Drift？", default=False)

    click.echo(f"\n{click.style('[4/5]', bold=True)} QQ / NapCat / OneBot（可跳过）\n")
    qq_enabled = click.confirm("配置 QQ/OneBot 频道？", default=False)
    qq_bot_uin = ""
    qq_api_base_url = "http://127.0.0.1:3000"
    qq_allow_from: list[str] = []
    qq_groups: list[str] = []
    qq_access_token = ""
    if qq_enabled:
        qq_bot_uin = click.prompt("Bot QQ 号")
        qq_api_base_url = click.prompt("OneBot HTTP API", default=qq_api_base_url)
        allow_csv = click.prompt("允许使用的 QQ 号（多个用逗号分隔）")
        qq_allow_from = [item.strip() for item in allow_csv.split(",") if item.strip()]
        qq_access_token = click.prompt(
            "OneBot access token（可留空）", default="", show_default=False, hide_input=True
        )
        group_csv = click.prompt(
            "允许使用的群号（多个用逗号分隔，可留空）",
            default="",
            show_default=False,
        )
        qq_groups = [item.strip() for item in group_csv.split(",") if item.strip()]
        onebot_error = _validate_onebot_api(qq_api_base_url, qq_access_token)
        if onebot_error:
            click.echo(click.style(f"  ! {onebot_error}", fg="yellow"))
            click.echo("  请确认 NapCat HTTP API 已启动；事件上报地址为 http://127.0.0.1:8766/qq/webhook")
        if qq_allow_from and not proactive_enabled:
            if click.confirm("开启 proactive 主动推送（via QQ/OneBot）？", default=True):
                proactive_enabled = True
                proactive_channel = "qq"
                proactive_chat_id = qq_allow_from[0]
                drift_enabled = click.confirm("主动链路空转时开启 Drift？", default=False)

    click.echo(f"\n{click.style('[5/5]', bold=True)} 生成配置与工作区\n")
    answers = WizardAnswers(
        workspace=workspace,
        model=model,
        base_url=base_url,
        api_key=api_key,
        context_window=context_window,
        telegram_enabled=telegram_enabled,
        telegram_token=telegram_token,
        telegram_allow_from=telegram_allow_from,
        proactive_enabled=proactive_enabled,
        proactive_channel=proactive_channel,
        proactive_chat_id=proactive_chat_id,
        qqbot_enabled=qqbot_enabled,
        qqbot_app_id=qqbot_app_id,
        qqbot_client_secret=qqbot_client_secret,
        qqbot_user_openid=qqbot_user_openid,
        qq_enabled=qq_enabled,
        qq_bot_uin=qq_bot_uin,
        qq_api_base_url=qq_api_base_url,
        qq_allow_from=qq_allow_from,
        qq_groups=qq_groups,
        qq_access_token=qq_access_token,
        drift_enabled=drift_enabled,
    )
    env_path = config_path.parent / ".env"
    _backup_existing(config_path)
    _backup_existing(env_path)
    _atomic_write(config_path, _render_config(answers), mode=0o600)
    _atomic_write(env_path, _render_env(answers), mode=0o600)
    initialize_workspace(config_path, workspace)
    click.echo(click.style("  ✓ 配置已生成", fg="green"))
    click.echo(click.style(f"  ✓ 工作区已初始化：{workspace}", fg="green"))
    click.echo("\n启动 agent：")
    click.echo(click.style("  uv run python main.py", bold=True))
    click.echo("Web Chat： http://127.0.0.1:6322")
