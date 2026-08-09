"""Canonical plugin runtime API."""

from agent.plugins.decorators import (
    on_after_reasoning,
    on_after_step,
    on_after_turn,
    on_before_reasoning,
    on_before_step,
    on_before_turn,
    on_prompt_render,
    on_tool_pre,
    tool,
)
from agent.plugins.manager import Plugin, PluginContext, PluginKVStore, PluginManager
from agent.plugins.specs import (
    ManagedServiceSpec,
    McpServerSpec,
    PluginReadinessContext,
    PluginSemanticCheck,
    ProactiveSourceSpec,
)

__all__ = [
    "ManagedServiceSpec",
    "McpServerSpec",
    "Plugin",
    "PluginContext",
    "PluginKVStore",
    "PluginManager",
    "PluginReadinessContext",
    "PluginSemanticCheck",
    "ProactiveSourceSpec",
    "on_after_reasoning",
    "on_after_step",
    "on_after_turn",
    "on_before_reasoning",
    "on_before_step",
    "on_before_turn",
    "on_prompt_render",
    "on_tool_pre",
    "tool",
]
