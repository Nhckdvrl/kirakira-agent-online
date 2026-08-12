"""Production composition root for the Cloud Agent worker."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent.config import config_value, load_dotenv, load_toml_config, require_env
from agent.pipeline_factory import PassivePipelineAssembly, build_passive_pipeline
from agent.tools import build_default_registry
from agent.tools.execution_backend import RemoteSandboxExecutionBackend
from bus.event_bus import EventBus
from bus.queue import MessageBus
from cloud.database import (
    CloudSettings,
    build_engine,
    build_session_factory,
    build_sync_engine,
)
from cloud.executor import CloudPipelineExecutor
from cloud.markdown_memory import (
    CloudMarkdownMemoryRuntime,
    build_cloud_markdown_memory_runtime,
)
from cloud.memory import CloudDefaultMemoryEngine, build_cloud_memory_services
from cloud.readiness import assess_cloud_worker_readiness
from cloud.store import CloudStore
from cloud.transcript import RunScopedTranscriptStore
from cloud.tool_checkpoints import build_cloud_tool_checkpoint_hooks
from cloud.run_stream import DurableRunStreamBridge
from cloud.worker import CloudWorker
from cloud.automation import CloudAutomationWorker
from cloud.scheduler import CloudScheduleWorker, CloudSchedulerTools
from cloud.credentials import CredentialVault
from cloud.mcp import CloudMcpCapabilities
from cloud.plugins import CloudPluginCapabilities, CloudPluginWorker
from cloud.subagents import CloudSubagentRuntime
from cloud.message_push import CloudMessagePushTool
from core.memory.markdown import MemoryLifecycleBindRequest
from core.memory.services import MemoryServices, memory_keep_count
from core.net.http import SharedHttpResources
from infra.providers.llm_provider import OpenAICompatibleClient
from infra.providers.model_client_adapter import ModelClientProvider


@dataclass(frozen=True)
class CloudMemoryContextView:
    """Prompt-block compatibility view backed by the Cloud profile store."""

    store: Any
    item_store: Any

    def list_records(self, *, include_forgotten: bool = False) -> list[dict]:
        rows, _ = self.item_store.list_items_for_dashboard(
            status="" if include_forgotten else "active",
            page=1,
            page_size=500,
        )
        return rows


@dataclass
class CloudWorkerRuntime:
    worker: CloudWorker
    executor: CloudPipelineExecutor
    assembly: PassivePipelineAssembly
    transcript_store: RunScopedTranscriptStore
    memory_services: MemoryServices
    markdown: CloudMarkdownMemoryRuntime
    execution_backend: RemoteSandboxExecutionBackend
    tools: Any
    async_engine: Any
    http_resources: SharedHttpResources
    stream_bridge: DurableRunStreamBridge
    automation_worker: CloudAutomationWorker
    schedule_worker: CloudScheduleWorker
    plugin_worker: CloudPluginWorker
    subagent_worker: CloudSubagentRuntime

    async def aclose(self) -> None:
        first_error: BaseException | None = None
        for close in (
            self.tools.shutdown,
            self.stream_bridge.close,
            self.memory_services.aclose,
            self.async_engine.dispose,
            self.http_resources.aclose,
        ):
            try:
                await close()
            except Exception as exc:  # noqa: BLE001 - close every owned resource
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error


async def build_cloud_worker_runtime(
    *,
    workspace: Path,
    app_config: dict[str, Any],
    settings: CloudSettings,
    execution_backend: RemoteSandboxExecutionBackend,
    worker_id: str,
    model_client: Any | None = None,
) -> CloudWorkerRuntime:
    """Assemble one production worker without Local state fallbacks."""
    model = os.getenv("MODEL_ID") or str(
        config_value(app_config, "llm", "main", "model", default="")
    )
    if not model:
        model = require_env("MODEL_ID")
    client = model_client or OpenAICompatibleClient(
        base_url=os.getenv("OPENAI_COMPATIBLE_BASE_URL")
        or config_value(app_config, "llm", "main", "base_url"),
        api_key=os.getenv("OPENAI_COMPATIBLE_API_KEY")
        or config_value(app_config, "llm", "main", "api_key", default=""),
        thinking_enabled=config_value(
            app_config, "llm", "main", "enable_thinking", default=None
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
    provider = ModelClientProvider(client)
    await execution_backend.probe()
    event_bus = EventBus()
    bus = MessageBus()
    transcripts = RunScopedTranscriptStore()
    http_resources = SharedHttpResources()
    async_engine = build_engine(settings, pool_pre_ping=True)
    store = CloudStore(build_session_factory(async_engine))
    stream_bridge = DurableRunStreamBridge(store)
    stream_bridge.bind(event_bus)
    sync_engine = build_sync_engine(settings, pool_pre_ping=True)

    markdown = build_cloud_markdown_memory_runtime(
        engine=sync_engine,
        provider=provider,
        model=model,
        keep_count=memory_keep_count(
            int(
                config_value(
                    app_config, "agent", "context", "memory_window", default=40
                )
            )
        ),
        event_bus=event_bus,
        recent_context_provider=provider,
        recent_context_model=model,
    )
    markdown.maintenance.bind_lifecycle(
        MemoryLifecycleBindRequest(
            get_session=transcripts.get_or_create,
            save_session=transcripts.save_async,
        )
    )
    memory_services = build_cloud_memory_services(
        app_config=app_config,
        settings=settings,
        workspace=workspace,
        provider=provider,
        light_provider=provider,
        http_resources=http_resources,
        event_publisher=event_bus,
        markdown=markdown,
        sync_engine=sync_engine,
    )
    memory_context = CloudMemoryContextView(markdown.store, memory_services.store)
    push_tool = CloudMessagePushTool(store)
    tools = build_default_registry(
        workspace,
        memory=memory_context,  # type: ignore[arg-type]
        session_manager=transcripts,  # type: ignore[arg-type]
        memory_services=memory_services,
        push_tool=push_tool,
        execution_backend=execution_backend,
        workspace_backend=execution_backend,
    )
    push_tool.attach_registry(tools)
    CloudSchedulerTools(store, tools)
    assembly = build_passive_pipeline(
        workspace=workspace,
        app_config=app_config,
        model=model,
        model_client=client,
        transcript_store=transcripts,
        memory=memory_context,
        memory_services=memory_services,
        tools=tools,
        bus=bus,
        event_bus=event_bus,
    )
    assembly.reasoner.add_tool_hooks(build_cloud_tool_checkpoint_hooks(store))
    memory_engine = memory_services.engine
    if not isinstance(memory_engine, CloudDefaultMemoryEngine):
        raise RuntimeError("Cloud worker requires CloudDefaultMemoryEngine")
    async def settle(conversation_id: str) -> None:
        await markdown.maintenance.wait_for_session(conversation_id)
        run_id = transcripts.current_run_id()
        if run_id is not None:
            await stream_bridge.flush_run(run_id)

    vault = CredentialVault()
    mcp_capabilities = CloudMcpCapabilities(store, vault)
    remote_capabilities = CloudPluginCapabilities(store, vault, mcp_capabilities)
    subagents = CloudSubagentRuntime(
        store=store,
        reasoner=assembly.reasoner,
        tools=tools,
        memory_engine=memory_engine,
        worker_id=f"{worker_id}:subagents",
        scope_binders=(
            memory_engine.bind_user,
            markdown.store.bind_user,
            push_tool.bind_user,
        ),
        capability_scope=remote_capabilities.for_user,
    )
    subagents.register_tools()
    executor = CloudPipelineExecutor(
        assembly.pipeline,
        transcripts,
        scope_binders=(
            memory_engine.bind_user,
            markdown.store.bind_user,
            push_tool.bind_user,
        ),
        settle=settle,
        capability_scope=remote_capabilities.for_user,
    )
    readiness = assess_cloud_worker_readiness(
        transcript_store=transcripts,
        memory_services=memory_services,
        execution_backend=execution_backend,
    )
    readiness.require_ready()
    worker = CloudWorker(store, executor, worker_id=worker_id)
    automation_worker = CloudAutomationWorker(
        store=store,
        sync_engine=sync_engine,
        transcripts=transcripts,
        memory_services=memory_services,
        markdown_store=markdown.store,
        model_client=client,
        model=model,
        app_config=app_config,
        workspace=workspace,
        execution_backend=execution_backend,
        worker_id=f"{worker_id}:automation",
    )
    schedule_worker = CloudScheduleWorker(
        store, worker_id=f"{worker_id}:scheduler"
    )
    plugin_worker = CloudPluginWorker(
        store,
        vault,
        worker_id=f"{worker_id}:plugins",
        model_client=client,
        model=model,
    )
    return CloudWorkerRuntime(
        worker=worker,
        executor=executor,
        assembly=assembly,
        transcript_store=transcripts,
        memory_services=memory_services,
        markdown=markdown,
        execution_backend=execution_backend,
        tools=tools,
        async_engine=async_engine,
        http_resources=http_resources,
        stream_bridge=stream_bridge,
        automation_worker=automation_worker,
        schedule_worker=schedule_worker,
        plugin_worker=plugin_worker,
        subagent_worker=subagents,
    )


async def build_cloud_worker_runtime_from_env() -> CloudWorkerRuntime:
    workspace = Path(os.getenv("KIRAKIRA_WORKSPACE", os.getcwd())).resolve()
    load_dotenv(workspace / ".env")
    config_path = Path(
        os.getenv("KIRAKIRA_CONFIG", str(workspace / "config.toml"))
    )
    app_config = load_toml_config(config_path)
    settings = CloudSettings.from_env()
    sandbox = RemoteSandboxExecutionBackend(
        require_env("KIRAKIRA_SANDBOX_URL"),
        auth_token=require_env("KIRAKIRA_SANDBOX_TOKEN"),
        timeout_seconds=float(os.getenv("KIRAKIRA_SANDBOX_TIMEOUT_SECONDS", "30")),
    )
    worker_id = os.getenv(
        "KIRAKIRA_WORKER_ID", f"{socket.gethostname()}:{os.getpid()}"
    )
    return await build_cloud_worker_runtime(
        workspace=workspace,
        app_config=app_config,
        settings=settings,
        execution_backend=sandbox,
        worker_id=worker_id,
    )
