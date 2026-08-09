"""Canonical construction for the passive Agent pipeline across all deployments."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent.config import config_value
from agent.core.runtime import DefaultReasoner, PassiveTurnPipeline, RuntimeConfig
from agent.looping.ports import ContextServices, SessionServices
from agent.model_runtime.context_policy import recommended_context_settings
from agent.prompting.context_builder import ContextBuilder


@dataclass(frozen=True)
class PassivePipelineAssembly:
    """The exact algorithm-bearing objects shared by Local and Cloud adapters."""

    context: ContextBuilder
    config: RuntimeConfig
    reasoner: DefaultReasoner
    pipeline: PassiveTurnPipeline


def build_passive_pipeline(
    *,
    workspace: Path,
    app_config: dict[str, Any],
    model: str,
    model_client: Any,
    transcript_store: Any,
    memory: Any,
    memory_services: Any,
    tools: Any,
    bus: Any,
    event_bus: Any,
    snapshot_store: Any = None,
    recall_inspector: Any = None,
) -> PassivePipelineAssembly:
    """Build the one canonical Context → Reasoner → Pipeline implementation.

    Deployment adapters supply identity, persistence, tools, delivery, and execution
    backends. They do not replace the reasoning or context algorithms constructed here.
    """
    context = ContextBuilder(
        workspace,
        memory,
        system_prompt=str(
            config_value(app_config, "agent", "system_prompt", default="")
        ),
    )
    context_window = int(
        config_value(app_config, "llm", "main", "context_window", default=128_000)
    )
    effective_context_percent = float(
        config_value(
            app_config,
            "agent",
            "context",
            "effective_context_percent",
            default=0.9,
        )
    )
    derived = recommended_context_settings(
        context_window,
        effective_context_percent,
    )
    runtime_config = RuntimeConfig(
        model=model,
        context_window=int(
            config_value(app_config, "llm", "main", "context_window", default=0)
        ),
        effective_context_percent=effective_context_percent,
        max_iterations=int(
            os.getenv(
                "AGENT_MAX_ITERATIONS",
                str(config_value(app_config, "agent", "max_iterations", default=10)),
            )
        ),
        max_tokens=int(
            os.getenv(
                "AGENT_MAX_TOKENS",
                str(
                    config_value(
                        app_config,
                        "agent",
                        "max_tokens",
                        default=derived.output_reserve,
                    )
                ),
            )
        ),
        history_window=int(
            os.getenv(
                "AGENT_HISTORY_WINDOW",
                str(
                    config_value(
                        app_config,
                        "agent",
                        "context",
                        "memory_window",
                        default=derived.memory_window,
                    )
                ),
            )
        ),
        model_timeout_seconds=float(os.getenv("AGENT_MODEL_TIMEOUT", "120")),
        repeated_tool_call_limit=int(os.getenv("AGENT_REPEATED_TOOL_LIMIT", "3")),
        stream=_env_bool("AGENT_STREAM", True),
    )
    reasoner = DefaultReasoner(
        model_client=model_client,
        tools=tools,
        config=runtime_config,
        context=context,
        event_bus=event_bus,
    )
    pipeline = PassiveTurnPipeline(
        bus=bus,
        event_bus=event_bus,
        session_manager=transcript_store,
        memory=memory,
        tools=tools,
        reasoner=reasoner,
        config=runtime_config,
        snapshot_store=snapshot_store,
        memory_services=memory_services,
        session_services=SessionServices(transcript_store=transcript_store),
        context_services=ContextServices(context=context),
        recall_inspector=recall_inspector,
    )
    return PassivePipelineAssembly(
        context=context,
        config=runtime_config,
        reasoner=reasoner,
        pipeline=pipeline,
    )


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on", "enabled")
