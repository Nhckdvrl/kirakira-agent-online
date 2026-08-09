"""Kirakira Agent CLI and local channel runtime."""

import asyncio
import argparse
import importlib.util
import os
import logging
import signal
import sys
from pathlib import Path
from typing import Any, List

from agent.core.runner import Agent, DEFAULT_SYSTEM
from agent.config import config_value, load_dotenv, load_toml_config, require_env
from bus.queue import MessageBus
from infra.channels.contract import ChannelContext
from infra.channels.host import ChannelHost
from infra.channels.qq_channel import QQChannel
from infra.channels.qqbot_channel import QQBotChannel
from infra.channels.telegram_channel import TelegramChannel
from infra.channels.web_chat_channel import WebChannel
from bus.event_bus import EventBus
from core.memory.legacy import MemoryRuntime
from agent.mcp import McpCatalogPublisher, WorkspaceMcpAdmin, WorkspaceMcpWatcher
from infra.providers.llm_provider import OpenAICompatibleClient
from infra.providers.model_client_adapter import ModelClientProvider
from core.memory.services import build_memory_services
from agent.pipeline_factory import build_passive_pipeline
from agent.plugins import PluginManager
from agent.plugins.watcher import PluginWatcher
from proactive_v2 import ProactiveLoop
from proactive_v2.config import ProactiveConfig
from plugins.wake_proactive.mcp_sources import (
    ToolRegistryMcpGateway,
    compile_proactive_sources,
)
from plugins.wake_proactive.sources import build_file_inbox_registry
from plugins.wake_proactive.state import ProactiveStateStore
from plugins.drift_flow import DriftRunner
from bootstrap.control import build_control_plane
from bootstrap.dashboard_api import DashboardService
from plugins.default_memory.inspector import RecallInspector
from agent.restart import RestartCoordinator, SupervisorCommitChannel
from agent.supervisor import RESTART_EXIT_CODE
from agent.core.runtime import (
    AgentLoop,
    CoreRuntime,
)
from core.schema import JsonDict
from session.manager import SessionManager
from agent.plugins.snapshot import RuntimeSnapshotStore
from agent.scheduler import SchedulerService
from agent.subagent import SubagentManager
from agent.skills import SkillLoader
from agent.tools import build_default_registry


def build_agent(workdir: Path) -> Agent:
    load_dotenv(workdir / ".env")
    app_config = load_toml_config(workdir / "config.toml")
    model = os.getenv("MODEL_ID") or str(
        config_value(app_config, "llm", "main", "model", default="")
    )
    if not model:
        model = require_env("MODEL_ID")
    client = OpenAICompatibleClient(
        base_url=os.getenv("OPENAI_COMPATIBLE_BASE_URL")
        or config_value(app_config, "llm", "main", "base_url"),
        api_key=os.getenv("OPENAI_COMPATIBLE_API_KEY")
        or config_value(app_config, "llm", "main", "api_key", default=""),
        thinking_enabled=config_value(
            app_config, "llm", "main", "enable_thinking"
        ),
        context_window=int(
            config_value(app_config, "llm", "main", "context_window", default=0)
        ),
        effective_context_percent=float(
            config_value(
                app_config,
                "agent",
                "context",
                "effective_context_percent",
                default=0.9,
            )
        ),
    )
    registry = build_default_registry(workdir)
    skills = SkillLoader(workdir / "skills")
    system = (
        DEFAULT_SYSTEM
        + "\nCurrent workspace: %s\nAvailable skills:\n%s" % (workdir, skills.descriptions())
    )
    return Agent(client, registry, model=model, workdir=workdir, system=system)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on", "enabled")


def _env_list(name: str) -> List[str]:
    value = os.getenv(name, "")
    return [item.strip() for item in value.split(",") if item.strip()]


def _build_channel_host(
    *,
    workdir: Path,
    bus: MessageBus,
    event_bus: EventBus,
    session_manager: SessionManager,
    enable_web: bool = False,
    enable_telegram: bool = False,
    enable_qq: bool = False,
    enable_qqbot: bool = False,
    interrupt=None,
    memory=None,
    app_config=None,
    dashboard=None,
    push_tool=None,
) -> ChannelHost | None:
    from agent.looping.interrupt import InterruptController
    from agent.tools.message_push import MessagePushTool
    from core.net.http import SharedHttpResources
    from infra.channels.base import AttachmentStore

    app_config = app_config or {}
    push_tool = push_tool or MessagePushTool(chat_lane=bus.chat_lane)
    interrupt_controller = InterruptController(interrupt) if interrupt else None
    attachment_store = AttachmentStore(workdir / "uploads")
    http_resources = SharedHttpResources()
    host = ChannelHost(
        lambda channel: ChannelContext(
            bus=bus,
            session_manager=session_manager,
            event_bus=event_bus,
            workspace=workdir,
            log=logging.getLogger("channels.%s" % channel.name),
            interrupt=interrupt,
            memory=memory,
            push_tool=push_tool,
            attachment_store=attachment_store,
            http_resources=http_resources,
            interrupt_controller=interrupt_controller,
        )
    )
    added = False
    chat_config = config_value(app_config, "channels", "chat", default={}) or {}
    if enable_web or _env_bool(
        "KIRAKIRA_WEB_ENABLED", bool(chat_config.get("enabled", False))
    ):
        host.add(
            WebChannel(
                host=os.getenv("KIRAKIRA_WEB_HOST", str(chat_config.get("host") or "127.0.0.1")),
                port=int(os.getenv("KIRAKIRA_WEB_PORT", str(chat_config.get("port") or 6322))),
                channel_name=os.getenv("KIRAKIRA_WEB_CHANNEL", str(chat_config.get("channel_name") or "web")),
                dashboard=dashboard,
            )
        )
        added = True
    telegram_config = config_value(app_config, "channels", "telegram", default={}) or {}
    telegram_token = (
        os.getenv("TELEGRAM_BOT_TOKEN")
        or str(telegram_config.get("token") or "")
    ).strip()
    if enable_telegram or _env_bool(
        "KIRAKIRA_TELEGRAM_ENABLED",
        bool(telegram_config.get("enabled", bool(telegram_token))),
    ):
        if not telegram_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is required when Telegram channel is enabled")
        host.add(
            TelegramChannel(
                token=telegram_token,
                bus=bus,
                session_manager=session_manager,
                allow_from=_env_list("TELEGRAM_ALLOW_FROM")
                or [str(item) for item in telegram_config.get("allow_from", [])],
                event_bus=event_bus,
                interrupt_controller=interrupt_controller,
                channel_name=os.getenv(
                    "KIRAKIRA_TELEGRAM_CHANNEL",
                    str(telegram_config.get("channel_name") or "telegram"),
                ),
            )
        )
        added = True
    qq_config = config_value(app_config, "channels", "qq", default={}) or {}
    qq_groups = qq_config.get("groups") or []
    configured_group_ids = [
        str(item.get("group_id"))
        for item in qq_groups
        if isinstance(item, dict) and item.get("group_id")
    ]
    group_policies = {
        str(item["group_id"]): {
            "allow_from": [str(user) for user in item.get("allow_from", [])],
            "require_at": bool(item.get("require_at", True)),
        }
        for item in qq_groups
        if isinstance(item, dict) and item.get("group_id")
    }
    if enable_qq or _env_bool(
        "KIRAKIRA_QQ_ENABLED",
        bool(qq_config.get("enabled", bool(qq_config.get("bot_uin")))),
    ):
        host.add(
            QQChannel(
                bot_uin=os.getenv("QQ_BOT_UIN", str(qq_config.get("bot_uin") or "")),
                api_base_url=os.getenv(
                    "ONEBOT_API_BASE_URL",
                    str(qq_config.get("api_base_url") or "http://127.0.0.1:3000"),
                ),
                webhook_host=os.getenv("KIRAKIRA_QQ_WEBHOOK_HOST", "127.0.0.1"),
                webhook_port=int(os.getenv("KIRAKIRA_QQ_WEBHOOK_PORT", "8766")),
                access_token=os.getenv(
                    "ONEBOT_ACCESS_TOKEN", str(qq_config.get("access_token") or "")
                ),
                allow_from=_env_list("QQ_ALLOW_FROM")
                or [str(item) for item in qq_config.get("allow_from", [])],
                group_allow=_env_list("QQ_GROUP_ALLOW") or configured_group_ids,
                group_policies=group_policies,
                require_at=_env_bool(
                    "QQ_REQUIRE_AT", bool(qq_config.get("require_at", True))
                ),
                channel_name=os.getenv(
                    "KIRAKIRA_QQ_CHANNEL", str(qq_config.get("channel_name") or "qq")
                ),
            )
        )
        added = True
    qqbot_config = config_value(app_config, "channels", "qqbot", default={}) or {}
    qqbot_app_id = (
        os.getenv("QQBOT_APP_ID") or str(qqbot_config.get("app_id") or "")
    ).strip()
    qqbot_secret = (
        os.getenv("QQBOT_CLIENT_SECRET")
        or str(qqbot_config.get("client_secret") or "")
    ).strip()
    if enable_qqbot or _env_bool(
        "KIRAKIRA_QQBOT_ENABLED",
        bool(qqbot_config.get("enabled", bool(qqbot_app_id and qqbot_secret))),
    ):
        if not qqbot_app_id or not qqbot_secret:
            raise RuntimeError(
                "QQBOT_APP_ID and QQBOT_CLIENT_SECRET are required when official QQBot is enabled"
            )
        host.add(
            QQBotChannel(
                app_id=qqbot_app_id,
                client_secret=qqbot_secret,
                allow_from=_env_list("QQBOT_ALLOW_FROM")
                or [str(item) for item in qqbot_config.get("allow_from", [])],
                channel_name=os.getenv(
                    "KIRAKIRA_QQBOT_CHANNEL",
                    str(qqbot_config.get("channel_name") or "qqbot"),
                ),
                api_base_url=str(
                    qqbot_config.get("api_base_url")
                    or "https://api.sgroup.qq.com"
                ),
            )
        )
        added = True
    return host if added else None


def register_agent_restart_tool(registry: Any, coordinator: Any) -> None:
    """注册 agent_restart(照 Reference agent/tools/agent_restart.py)。

    只在 supervisor 托管时注册;deferred 对应 Reference 的 requires_turn_search 边界
    (模型必须先 tool_search 发现它,不进默认工具面)。turn 上下文取自 ContextVar 与
    registry context;非控制面 turn 没有 current_turn_id,arm 会明确拒绝而不是挂起。
    """
    import json as _json

    from agent.control.context import current_turn_id
    from core.schema import ToolSpec
    from agent.tools.registry import object_schema

    async def agent_restart(reason: str) -> str:
        ctx = registry.context
        request = coordinator.arm(
            turn_id=current_turn_id.get(),
            session_key=str(ctx.get("session_key") or ""),
            channel=str(ctx.get("channel") or ""),
            chat_id=str(ctx.get("chat_id") or ""),
            reason=reason,
        )
        return _json.dumps(
            {
                "status": "scheduled",
                "requestId": request.id,
                "message": "将在本轮回复持久化并送达后重启。",
            },
            ensure_ascii=False,
        )

    registry.register(
        ToolSpec(
            "agent_restart",
            "安全重启当前 Kirakira Agent。仅在核心代码或主配置必须重新加载时使用；"
            "MCP 与插件可热重载时不要调用。执行后会等待本轮回复持久化并送达。",
            object_schema(
                {
                    "reason": {
                        "type": "string",
                        "description": "需要完整重启 Agent 的具体原因",
                        "minLength": 1,
                        "maxLength": 300,
                    }
                },
                ["reason"],
            ),
        ),
        agent_restart,
        deferred=True,
    )


def _build_source_registry(
    workdir: Path,
    plugin_sources: Any = None,
    tool_registry: Any = None,
    gateway: Any = None,
):
    """内置文件源 + 插件声明编译出的 MCP 源。

    插件只声明 ProactiveSourceSpec,由这里编译成真实 ProactiveSource——
    这是主动链路可扩展的接线点。单个源注册失败不阻断其余源与整条链路。
    """
    registry = build_file_inbox_registry(workdir)
    for source in compile_proactive_sources(
        list(plugin_sources or []), tool_registry, gateway=gateway
    ):
        try:
            registry.add(source)
        except ValueError as error:
            logging.getLogger(__name__).warning(
                "plugin proactive source skipped: %s", error
            )
    return registry


def _build_proactive(
    *,
    workdir: Path,
    app_config: JsonDict,
    default_model: str,
    bus: MessageBus,
    session_manager: SessionManager,
    memory: MemoryRuntime,
    client: OpenAICompatibleClient,
    passive_busy_fn: Any | None = None,
    memory_services: Any = None,
    plugin_sources: Any = None,
    tool_registry: Any = None,
    plugin_generations: Any = None,
    snapshot_store: Any = None,
    drift_skill_roots_provider: Any = None,
) -> tuple[ProactiveLoop | None, DriftRunner | None]:
    """按配置装配主动推送链路与 Drift 链路；未启用则返回 (None, None)。"""
    cfg = ProactiveConfig.from_app_config(app_config, default_model=default_model)
    if not cfg.enabled:
        return None, None
    if not cfg.target_ready:
        logging.getLogger(__name__).warning(
            "[proactive] enabled=true 但未配置 target.channel/chat_id，主动链路不启动"
        )
        return None, None

    # 长期档案读取绑定 coremem 的 markdown store。Reference 里 Drift/主动读的
    # 就是 MEMORY.md(MemoryProfileApi),不是引擎;这里同口径,不再经过旧 MemoryRuntime。
    markdown_runtime = getattr(memory_services, "markdown", None)
    long_term_source = getattr(markdown_runtime, "store", None) or memory

    drift_runner: DriftRunner | None = None
    drift_hook = None
    if cfg.drift.enabled:
        drift_runner = DriftRunner(
            config=cfg.drift,
            workspace=workdir,
            bus=bus,
            session_manager=session_manager,
            model_client=client,
            model=cfg.model,
            memory=long_term_source,
            target_channel=cfg.channel,
            target_chat_id=cfg.chat_id,
            skill_roots_provider=drift_skill_roots_provider,
        )
        drift_hook = drift_runner.maybe_run

    # 所有插件源共用一个 gateway;tick 开始时 loop 把租到的快照钉在上面,
    # 本轮 fetch/ack 用同一代 MCP 工具视图。
    mcp_gateway = ToolRegistryMcpGateway(tool_registry)
    loop = ProactiveLoop(
        config=cfg,
        bus=bus,
        session_manager=session_manager,
        model_client=client,
        sources=_build_source_registry(
            workdir, plugin_sources, tool_registry, gateway=mcp_gateway
        ),
        state=ProactiveStateStore(workdir / "proactive.db"),
        memory=long_term_source,
        drift_hook=drift_hook,
        passive_busy_fn=passive_busy_fn,
        memory_services=memory_services,
        plugin_generations=plugin_generations,
        snapshot_store=snapshot_store,
        mcp_gateway=mcp_gateway,
    )
    return loop, drift_runner


async def build_runtime(
    workdir: Path,
    *,
    enable_web: bool = False,
    enable_telegram: bool = False,
    enable_qq: bool = False,
    enable_qqbot: bool = False,
    config_path: Path | None = None,
) -> CoreRuntime:
    load_dotenv(workdir / ".env")
    app_config = load_toml_config(config_path or workdir / "config.toml")
    # 主动目标必须有真实 Channel subscriber。配置 proactive target 后自动装配对应
    # 内置渠道，避免用户还要额外猜测 --telegram/--qq/--web 才能完成发送链。
    proactive_enabled = bool(
        config_value(app_config, "proactive", "enabled", default=False)
    )
    proactive_channel = str(
        config_value(app_config, "proactive", "target", "channel", default="")
        or ""
    ).strip()
    if proactive_enabled:
        web_cfg = config_value(app_config, "channels", "chat", default={}) or {}
        telegram_cfg = (
            config_value(app_config, "channels", "telegram", default={}) or {}
        )
        qq_cfg = config_value(app_config, "channels", "qq", default={}) or {}
        qqbot_cfg = config_value(app_config, "channels", "qqbot", default={}) or {}
        web_name = os.getenv(
            "KIRAKIRA_WEB_CHANNEL", str(web_cfg.get("channel_name") or "web")
        )
        telegram_name = os.getenv(
            "KIRAKIRA_TELEGRAM_CHANNEL",
            str(telegram_cfg.get("channel_name") or "telegram"),
        )
        qq_name = os.getenv(
            "KIRAKIRA_QQ_CHANNEL", str(qq_cfg.get("channel_name") or "qq")
        )
        qqbot_name = os.getenv(
            "KIRAKIRA_QQBOT_CHANNEL",
            str(qqbot_cfg.get("channel_name") or "qqbot"),
        )
        enable_web = enable_web or proactive_channel == web_name
        enable_telegram = enable_telegram or proactive_channel == telegram_name
        enable_qq = enable_qq or proactive_channel == qq_name
        enable_qqbot = enable_qqbot or proactive_channel == qqbot_name
    model = os.getenv("MODEL_ID") or str(
        config_value(app_config, "llm", "main", "model", default="")
    )
    if not model:
        model = require_env("MODEL_ID")
    client = OpenAICompatibleClient(
        base_url=os.getenv("OPENAI_COMPATIBLE_BASE_URL")
        or config_value(app_config, "llm", "main", "base_url"),
        api_key=os.getenv("OPENAI_COMPATIBLE_API_KEY")
        or config_value(app_config, "llm", "main", "api_key", default=""),
        thinking_enabled=config_value(
            app_config, "llm", "main", "enable_thinking"
        ),
        context_window=int(
            config_value(app_config, "llm", "main", "context_window", default=0)
        ),
        effective_context_percent=float(
            config_value(
                app_config,
                "agent",
                "context",
                "effective_context_percent",
                default=0.9,
            )
        ),
    )
    memory_client: Any = client
    light_model = os.getenv("LIGHT_MODEL_ID") or str(
        config_value(app_config, "llm", "light", "model", default="") or ""
    )
    light_base_url = os.getenv("LIGHT_OPENAI_COMPATIBLE_BASE_URL") or str(
        config_value(app_config, "llm", "light", "base_url", default="") or ""
    )
    if light_model and light_base_url:
        from agent.model_runtime.fallback import ResilientModelClient

        light_client = OpenAICompatibleClient(
            base_url=light_base_url,
            api_key=os.getenv("LIGHT_OPENAI_COMPATIBLE_API_KEY")
            or config_value(app_config, "llm", "light", "api_key", default="")
            or os.getenv("OPENAI_COMPATIBLE_API_KEY")
            or config_value(app_config, "llm", "main", "api_key", default=""),
            thinking_enabled=config_value(
                app_config, "llm", "light", "enable_thinking", default=False
            ),
            context_window=int(
                config_value(
                    app_config,
                    "llm",
                    "light",
                    "context_window",
                    default=config_value(
                        app_config, "llm", "main", "context_window", default=0
                    ),
                )
            ),
            effective_context_percent=float(
                config_value(
                    app_config,
                    "agent",
                    "context",
                    "effective_context_percent",
                    default=0.9,
                )
            ),
        )
        memory_client = ResilientModelClient(
            primary=light_client,
            primary_runtime_id="light",
            primary_model=light_model,
            fallback=client,
            fallback_model=model,
        )
    bus = MessageBus()
    from agent.tools.message_push import MessagePushTool

    push_tool = MessagePushTool(chat_lane=bus.chat_lane)
    event_bus = EventBus()
    session_manager = SessionManager(workdir)
    # 记忆 DI 缝先建立:引擎是 coremem.db 的唯一 owner,旧 MemoryRuntime 共享它的连接,
    # 也让显式记忆工具(Stage 5)能在注册表构建时就拿到引擎。
    memory_provider = ModelClientProvider(memory_client)
    memory_services = build_memory_services(
        app_config=app_config,
        workspace=workdir,
        provider=memory_provider,
        light_provider=memory_provider,
        event_publisher=event_bus,
        session_manager=session_manager,
        memory_window=int(
            config_value(app_config, "agent", "context", "memory_window", default=40)
        ),
    )
    memory = MemoryRuntime(
        workdir,
        session_manager=session_manager,
        engine=str(config_value(app_config, "memory", "engine", default="auto")),
        shared_store=memory_services.store,
        event_bus=event_bus,
    )
    embedding_model = os.getenv("EMBEDDING_MODEL_ID") or str(
        config_value(app_config, "memory", "embedding", "model", default="")
    )
    embedding_base_url = os.getenv("EMBEDDING_BASE_URL") or str(
        config_value(app_config, "memory", "embedding", "base_url", default="")
    )
    if embedding_model and embedding_base_url:
        memory.configure_embeddings(
            model=embedding_model,
            base_url=embedding_base_url,
            api_key=os.getenv("EMBEDDING_API_KEY")
            or str(
                config_value(
                    app_config, "memory", "embedding", "api_key", default=""
                )
            ),
        )
    registry = build_default_registry(
        workdir,
        memory=memory,
        session_manager=session_manager,
        bus=bus,
        push_tool=push_tool,
        memory_services=memory_services,
    )
    # 能力快照：MCP 换代只切换 current 快照，在途 turn 用完旧租约后旧进程才断开。
    snapshot_store = RuntimeSnapshotStore()
    mcp_publisher = McpCatalogPublisher(snapshot_store)
    snapshot_store.set_drain_handler(mcp_publisher.drain_snapshot)
    # workspace MCP 由 mcp/servers/*.toml 声明并热重载；首轮 reconcile 失败不阻塞启动，
    # watcher 会在声明修好后自动重试。
    mcp_watcher = WorkspaceMcpWatcher(workdir / "mcp" / "servers", mcp_publisher)
    # 让 agent 也能自己增删声明，走的仍是 watcher 那条 reconcile 路径。
    WorkspaceMcpAdmin(workdir, mcp_watcher).register_tools(registry)
    try:
        await mcp_watcher.reconcile()
    except (OSError, ValueError, RuntimeError) as error:
        mcp_watcher.last_error = str(error)
        logging.getLogger(__name__).error("workspace MCP initial publish failed: %s", error)
    assembly = build_passive_pipeline(
        workspace=workdir,
        app_config=app_config,
        model=model,
        model_client=client,
        transcript_store=session_manager,
        memory=memory,
        memory_services=memory_services,
        tools=registry,
        bus=bus,
        event_bus=event_bus,
        snapshot_store=snapshot_store,
        recall_inspector=RecallInspector(workdir),
    )
    context = assembly.context
    reasoner = assembly.reasoner
    pipeline = assembly.pipeline
    subagents = SubagentManager(
        reasoner=reasoner,
        tools=registry,
        sessions=session_manager,
        memory=memory,
        bus=bus,
    )
    subagents.register_tool()
    scheduler = SchedulerService(
        workdir / ".kirakira" / "schedules.json",
        bus=bus,
        tools=registry,
    )
    # 检索回放:记录每轮召回了什么、注入了没有;引擎未承重时这条路本来就不走。
    recall_inspector = pipeline.recall_inspector
    reasoner.add_tool_hooks([recall_inspector.tool_hook()])
    loop = AgentLoop(bus=bus, pipeline=pipeline)
    bundled_plugin_root = (
        Path(__file__).resolve().parents[1] / "plugin_packages" / "curated_feeds"
    )
    plugin_roots = [workdir / ".kirakira" / "plugins"]
    if bundled_plugin_root.is_dir():
        plugin_roots.append(bundled_plugin_root)
    plugin_manager = PluginManager(
        plugin_roots,
        event_bus=event_bus,
        tool_registry=registry,
        workspace=workdir,
        session_manager=session_manager,
        memory=memory,
        mcp_publisher=mcp_publisher,
        skill_loader=context.skills,
    )
    await plugin_manager.load_all()
    # 在途 turn 持有各插件当前代际租约;热重载换代不会抽走 turn 正在用的能力。
    pipeline.plugin_generations = plugin_manager.generations
    plugin_watcher = PluginWatcher(plugin_manager)
    # 安装/卸载/启停后立即热重载,不必重启进程。
    plugin_manager.reload_hook = plugin_watcher.wake
    reasoner.add_tool_hooks(plugin_manager.tool_hooks)
    reasoner.add_prompt_render_plugin_modules(plugin_manager.prompt_render_modules)
    reasoner.add_before_step_plugin_modules(plugin_manager.before_step_modules)
    reasoner.add_after_step_plugin_modules(plugin_manager.after_step_modules)
    pipeline.add_before_turn_plugin_modules(plugin_manager.before_turn_modules)
    pipeline.add_before_reasoning_plugin_modules(plugin_manager.before_reasoning_modules)
    pipeline.add_after_reasoning_plugin_modules(plugin_manager.after_reasoning_modules)
    pipeline.add_after_turn_plugin_modules(plugin_manager.after_turn_modules)
    # Dashboard 数据面。主动/Drift 在下面才装配,这里先建再回填——DashboardService
    # 是可变 dataclass,这样不必为了一个只读面板重排整条装配顺序。
    dashboard = DashboardService(
        workspace=workdir,
        session_manager=session_manager,
        memory_services=memory_services,
        memory=memory,
        plugin_manager=plugin_manager,
        restart_coordinator=None,
        recall_inspector=recall_inspector,
        # 状态库连接归本循环所在线程独占;Web 的 HTTP handler 在别的线程,读要 marshal 回来。
        loop=asyncio.get_running_loop(),
    )
    channel_host = _build_channel_host(
        workdir=workdir,
        bus=bus,
        event_bus=event_bus,
        session_manager=session_manager,
        enable_web=enable_web,
        enable_telegram=enable_telegram,
        enable_qq=enable_qq,
        enable_qqbot=enable_qqbot,
        interrupt=loop.request_interrupt,
        memory=memory,
        app_config=app_config,
        dashboard=dashboard,
        push_tool=push_tool,
    )
    if plugin_manager.channels:
        if channel_host is None:
            channel_host = ChannelHost(
                lambda channel: ChannelContext(
                    bus=bus,
                    session_manager=session_manager,
                    event_bus=event_bus,
                    workspace=workdir,
                    log=logging.getLogger("channels.%s" % channel.name),
                    interrupt=loop.request_interrupt,
                    memory=memory,
                    push_tool=push_tool,
                )
            )
        for plugin_channel in plugin_manager.channels:
            channel_host.add(plugin_channel)
    proactive_loop, drift_runner = _build_proactive(
        workdir=workdir,
        app_config=app_config,
        default_model=model,
        bus=bus,
        session_manager=session_manager,
        memory=memory,
        client=client,
        passive_busy_fn=loop.is_busy,
        memory_services=memory_services,
        plugin_sources=plugin_manager.proactive_sources,
        tool_registry=registry,
        # tick 与被动 turn 同一对租约保证:换代不抽走在途能力。
        plugin_generations=plugin_manager.generations,
        snapshot_store=snapshot_store,
        drift_skill_roots_provider=lambda: plugin_manager.drift_skill_roots,
    )
    # 回填晚于面板构造的两条链路,Dashboard 的主动/Drift 面板由此拿到真实数据。
    dashboard.proactive_loop = proactive_loop
    dashboard.drift_runner = drift_runner
    if proactive_loop is not None:
        available_channels = {
            channel.name for channel in (channel_host.channels if channel_host else [])
        }
        target_channel = proactive_loop.target_channel
        if target_channel not in available_channels:
            raise RuntimeError(
                "proactive target channel %r is not configured; available channels: %s"
                % (target_channel, ", ".join(sorted(available_channels)) or "(none)")
            )
    async def _control_consolidate(thread_id: str) -> bool:
        """thread/consolidate/start 的执行体:强制归档一个 thread 并报告是否有变化。"""
        maintenance = getattr(memory_services, "markdown", None)
        maintenance = getattr(maintenance, "maintenance", None)
        if maintenance is None:
            raise RuntimeError("当前 runtime 没有配置记忆归档能力")
        from core.memory.markdown import ConsolidateRequest

        session = session_manager.get_or_create(thread_id)
        before = int(session.last_consolidated or 0)
        await maintenance.consolidate(
            ConsolidateRequest(
                session=session,
                force=True,
                scope_channel=str(session.metadata.get("channel") or ""),
                scope_chat_id=str(session.metadata.get("chat_id") or ""),
            )
        )
        changed = int(session.last_consolidated or 0) > before
        if changed:
            await session_manager.save_async(session)
        return changed

    # supervisor 托管时装配重启协调器:supervisor(Reference 原文)早已在等私有管道上
    # 唯一一帧 restart_commit + 退出码 75,这里补进程内的另一半。非托管运行为 None,
    # agent_restart 不注册,准入永远开放。声明 supervised 却缺 fd 是环境契约被破坏,
    # from_environment 直接抛错(fail loud),不静默降级。
    restart_coordinator = None
    commit_channel = SupervisorCommitChannel.from_environment()
    if commit_channel is not None:
        restart_coordinator = RestartCoordinator(
            commit_channel.boot_id,
            supervised=True,
            commit=commit_channel.commit,
        )

    # 控制面:workspace 私有 Unix socket 上的 JSON-RPC,让外部程序观测/驱动 agent。
    control_store, control_runtime, control_service, control_server = (
        build_control_plane(
            workspace=workdir,
            pipeline=pipeline,
            sessions=session_manager,
            endpoint=os.getenv("KIRAKIRA_CONTROL_ENDPOINT", "").strip() or None,
            workspace_token=os.getenv("KIRAKIRA_CONTROL_TOKEN", "").strip() or None,
            boot_id=commit_channel.boot_id if commit_channel is not None else None,
            plugin_drain=plugin_manager.reconcile_disabled_and_drain,
            consolidate=_control_consolidate,
            restart_coordinator=restart_coordinator,
        )
    )
    dashboard.restart_coordinator = restart_coordinator
    if restart_coordinator is not None:
        register_agent_restart_tool(registry, restart_coordinator)
    return CoreRuntime(
        bus=bus,
        event_bus=event_bus,
        session_manager=session_manager,
        memory=memory,
        tools=registry,
        context=context,
        reasoner=reasoner,
        pipeline=pipeline,
        loop=loop,
        channel_host=channel_host,
        plugin_manager=plugin_manager,
        mcp_watcher=mcp_watcher,
        plugin_watcher=plugin_watcher,
        memory_services=memory_services,
        scheduler=scheduler,
        subagents=subagents,
        proactive_loop=proactive_loop,
        drift_runner=drift_runner,
        control_store=control_store,
        control_runtime=control_runtime,
        control_service=control_service,
        control_server=control_server,
        restart_coordinator=restart_coordinator,
    )


def print_response_text(response_text: str) -> None:
    if response_text:
        print(response_text)


def repl(agent: Agent, workdir: Path) -> None:
    history: List[JsonDict] = []
    skill_loader = SkillLoader(workdir / "skills")
    print("kirakira-agent ready. /tools /skills /compact /exit")
    while True:
        try:
            query = input("kirakira >> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not query:
            continue
        if query in ("/exit", "exit", "q", "quit"):
            break
        if query == "/tools":
            print("\n".join(agent.tool_registry.names()))
            continue
        if query == "/skills":
            skill_loader.reload()
            print(skill_loader.descriptions())
            continue
        if query == "/compact":
            if history:
                history[:] = agent.compact(history)
                print("Context compacted.")
            else:
                print("No context to compact.")
            continue

        history.append({"role": "user", "content": query})
        try:
            response = agent.run(history)
        except RuntimeError as exc:
            print("Error: %s" % exc)
            continue
        print_response_text(response.text)


async def runtime_repl(
    runtime: CoreRuntime, workdir: Path, session_id: str | None = None
) -> None:
    """Backward-compatible name for the line-oriented streaming client."""

    from frontend.tui.plain import runtime_plain_repl

    await runtime_plain_repl(runtime, workdir, session_id=session_id)


async def runtime_serve(runtime: CoreRuntime) -> int:
    """跑后台服务直到停止信号或 agent 自请求重启。

    返回进程退出码:重启提交成立时返回 RESTART_EXIT_CODE(75),supervisor 收到
    75 + 私有管道上的有效 commit 帧才会拉起下一代(照 Reference main.py:serve)。
    """
    tasks = await runtime.start_background()
    readiness = None
    boot_id = (
        os.getenv("AKASHIC_BOOT_ID", "")
        or os.getenv("KIRAKIRA_BOOT_ID", "")
    ).strip()
    supervised = (
        os.getenv("AKASHIC_SUPERVISED") == "1"
        or os.getenv("KIRAKIRA_SUPERVISED") == "1"
    )
    if supervised and boot_id:
        from bootstrap.runtime_readiness import RuntimeReadiness

        readiness = RuntimeReadiness(runtime.session_manager.workspace, boot_id)
        readiness.mark_ready()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    registered_signals: list[signal.Signals] = []
    for watched in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(watched, stop_event.set)
            registered_signals.append(watched)
        except (NotImplementedError, RuntimeError):
            pass
    restart_requested = False
    stop_task = asyncio.create_task(stop_event.wait(), name="shutdown_signal")
    restart_task = (
        asyncio.create_task(
            runtime.restart_coordinator.wait_committed(),
            name="restart_committed",
        )
        if runtime.restart_coordinator is not None
        else None
    )
    try:
        print("kirakira-agent server running. Ctrl+C to stop.")
        watched_tasks = {stop_task}
        if restart_task is not None:
            watched_tasks.add(restart_task)
        done, _ = await asyncio.wait(
            watched_tasks, return_when=asyncio.FIRST_COMPLETED
        )
        if restart_task is not None and restart_task in done:
            await restart_task
            restart_requested = True
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        for pending_task in (stop_task, restart_task):
            if pending_task is not None and not pending_task.done():
                pending_task.cancel()
        for watched in registered_signals:
            loop.remove_signal_handler(watched)
        if readiness is not None:
            readiness.clear()
        await runtime.stop_background(tasks)
    return RESTART_EXIT_CODE if restart_requested else 0


def resolve_workspace(
    cli_workspace: str | None,
    app_config: JsonDict,
    *,
    default: Path,
) -> Path:
    """按 --workspace > KIRAKIRA_WORKSPACE > config [runtime].workspace > 默认解析。

    运行时可写状态（session、记忆、附件、插件数据、workspace MCP）全部落在这个根下，
    不同 workspace 之间互不共享。
    """

    env_value = os.getenv("KIRAKIRA_WORKSPACE")
    configured = str(config_value(app_config, "runtime", "workspace", default="") or "")
    for candidate in (cli_workspace, env_value, configured):
        if candidate is None:
            continue
        text = str(candidate).strip()
        if not text:
            continue
        return Path(text).expanduser().resolve()
    return default


async def _main_async(args: argparse.Namespace, workdir: Path) -> int:
    from bootstrap.workspace_lock import WorkspaceInstanceLock

    workspace_lock = WorkspaceInstanceLock(workdir)
    workspace_lock.acquire()
    try:
        runtime = await build_runtime(
            workdir,
            enable_web=args.web,
            enable_telegram=args.telegram,
            enable_qq=args.qq,
            enable_qqbot=args.qqbot,
            config_path=args.config_path,
        )
        if getattr(args, "proactive", False):
            await _run_proactive_once(runtime)
        elif args.serve or args.web or args.telegram or args.qq or args.qqbot:
            return await runtime_serve(runtime)
        else:
            mode = choose_cli_mode(force_tui=args.tui, force_plain=args.plain)
            if mode == "tui":
                try:
                    from frontend.tui.app import runtime_tui
                except ModuleNotFoundError as exc:
                    if exc.name != "textual":
                        raise
                    print(
                        "Textual 未安装，自动回退到 plain 流式模式。"
                        "安装项目依赖后可使用全屏 TUI。",
                        file=sys.stderr,
                    )
                    await runtime_repl(runtime, workdir, session_id=args.session)
                else:
                    await runtime_tui(runtime, workdir, session_id=args.session)
            else:
                await runtime_repl(runtime, workdir, session_id=args.session)
        return 0
    finally:
        workspace_lock.release()


async def _run_proactive_once(runtime: CoreRuntime) -> None:
    """手动跑一次主动 tick 并打印状态，供演示/调试（不等电量定时器）。"""
    import json as _json

    loop = runtime.proactive_loop
    if loop is None:
        print(
            "主动链路未启用。请在 config.toml 的 [proactive] 设 enabled=true "
            "并填好 [proactive.target]。"
        )
        return
    # 单次模式也必须启动真实 Channel；只跑 Bus dispatcher 只能入队，无法完成发送。
    if runtime.channel_host is not None:
        await runtime.channel_host.start_all()
    tasks = [asyncio.create_task(runtime.bus.dispatch_outbound(), name="bus_dispatch")]
    try:
        print("→ 执行一次主动 tick ...")
        await loop.tick_once()
        await runtime.bus.drain(timeout=10.0)
        print(_json.dumps(loop.status(), ensure_ascii=False, indent=2))
    finally:
        # stop_background 负责关闭所有资源，并取消我们起的 dispatch task。
        await runtime.stop_background(tasks)


def choose_cli_mode(
    *,
    force_tui: bool = False,
    force_plain: bool = False,
    stdin_isatty: bool | None = None,
    stdout_isatty: bool | None = None,
    textual_available: bool | None = None,
) -> str:
    """Select a UI without importing optional TUI dependencies in scripts/CI."""

    if force_plain:
        return "plain"
    if force_tui:
        return "tui"
    stdin_tty = sys.stdin.isatty() if stdin_isatty is None else stdin_isatty
    stdout_tty = sys.stdout.isatty() if stdout_isatty is None else stdout_isatty
    available = (
        importlib.util.find_spec("textual") is not None
        if textual_available is None
        else textual_available
    )
    return "tui" if stdin_tty and stdout_tty and available else "plain"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Kirakira Agent")
    parser.add_argument(
        "command",
        nargs="?",
        choices=["memory"],
        help="Administrative command group (currently: memory).",
    )
    parser.add_argument(
        "memory_action",
        nargs="?",
        choices=["doctor", "backup", "migrate", "verify", "rollback", "clear", "repair-kinds"],
        help="Memory administration action.",
    )
    parser.add_argument("--backup-id", default="", help="Backup id for memory verify/rollback.")
    parser.add_argument("--confirm", default="", help="Explicit confirmation token for destructive memory actions.")
    parser.add_argument("--include-sessions", action="store_true", help="Also delete all persisted sessions during memory clear.")
    parser.add_argument("--clear-self", action="store_true", help="Also reset memory/SELF.md during memory clear.")
    parser.add_argument("--dry-run", action="store_true", help="Report planned changes without writing (memory repair-kinds).")
    parser.add_argument("--serve", action="store_true", help="Run background agent loop and configured channels.")
    parser.add_argument("--web", action="store_true", help="Enable stdlib web channel.")
    parser.add_argument("--telegram", action="store_true", help="Enable Telegram Bot API channel.")
    parser.add_argument("--qq", action="store_true", help="Enable QQ OneBot webhook channel.")
    parser.add_argument("--qqbot", action="store_true", help="Enable official Tencent QQBot channel.")
    parser.add_argument(
        "--proactive",
        action="store_true",
        help="Run one proactive tick now (demo/debug), print status, then exit.",
    )
    ui_group = parser.add_mutually_exclusive_group()
    ui_group.add_argument(
        "--tui",
        action="store_true",
        help="Force the full-screen Textual client for local interaction.",
    )
    ui_group.add_argument(
        "--plain",
        action="store_true",
        help="Use the line-oriented streaming client (also selected for pipes/CI).",
    )
    parser.add_argument(
        "--session",
        default=None,
        help=(
            "Named local conversation to resume. Without this flag, each launch "
            "starts a fresh empty conversation."
        ),
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help=(
            "Runtime state root (sessions, memory, plugin data, workspace MCP). "
            "Overrides KIRAKIRA_WORKSPACE and config [runtime].workspace."
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.toml. Defaults to ./config.toml.",
    )
    args = parser.parse_args(argv)
    # config 先于 workspace 解析：workspace 可以写在 config 里，但 config 本身不住在
    # workspace 内，否则会形成先有鸡还是先有蛋。
    cwd = Path(os.getcwd()).resolve()
    args.config_path = (
        Path(args.config).expanduser().resolve() if args.config else cwd / "config.toml"
    )
    # 解析 workspace 就要展开 config 里的 ${...} 引用（含 secret），所以先加载与
    # config 同目录的 .env；build_agent 之后会再按 workspace 加载一次，setdefault 保证
    # 早绑定的值不被覆盖。
    load_dotenv(args.config_path.parent / ".env")
    workdir = resolve_workspace(
        args.workspace, load_toml_config(args.config_path), default=cwd
    )
    workdir.mkdir(parents=True, exist_ok=True)
    if not (
        os.environ.get("AKASHIC_SUPERVISED") == "1"
        or os.environ.get("KIRAKIRA_SUPERVISED") == "1"
    ):
        from agent.migrations import migrate_installation

        try:
            outcome = migrate_installation(args.config_path, workdir)
        except RuntimeError as exc:
            parser.exit(2, "workspace migration failed: %s\n" % exc)
        if outcome.state == "migrated":
            print("workspace migrations applied: %s" % len(outcome.migrations))
    if args.command == "memory":
        import json as _json

        from bootstrap.memory_admin import (
            backup, clear, doctor, migrate, repair_kinds, rollback, verify,
        )

        if not args.memory_action:
            parser.error("memory requires doctor/backup/migrate/verify/rollback/clear/repair-kinds")
        try:
            if args.memory_action == "doctor":
                result = doctor(workdir, project_root=cwd)
            elif args.memory_action == "backup":
                result = backup(workdir)
            elif args.memory_action == "migrate":
                result = migrate(workdir)
            elif args.memory_action == "verify":
                result = verify(workdir, backup_id=args.backup_id)
            elif args.memory_action == "repair-kinds":
                result = repair_kinds(workdir, dry_run=args.dry_run)
            elif args.memory_action == "rollback":
                result = rollback(workdir, backup_id=args.backup_id)
            else:
                result = clear(
                    workdir,
                    confirm=args.confirm,
                    include_sessions=args.include_sessions,
                    clear_self=args.clear_self,
                )
        except Exception as exc:
            parser.exit(2, "memory %s failed: %s\n" % (args.memory_action, exc))
        print(_json.dumps(result, ensure_ascii=False, indent=2))
        return
    exit_code = asyncio.run(_main_async(args, workdir))
    if exit_code:
        # 重启换代(75)必须作为进程退出码传给 supervisor;正常退出保持 return 语义。
        sys.exit(exit_code)
