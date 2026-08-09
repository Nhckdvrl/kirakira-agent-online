"""Reference-style proactive source plus plugin-distributed Drift skills."""

from __future__ import annotations

from typing import Any, List

from agent.plugins.specs import (
    McpServerSpec,
    PluginSemanticCheck,
    ProactiveSourceSpec,
)
from agent.plugins import Plugin


class CuratedFeedsConfig:
    def __init__(
        self,
        proactive: dict[str, Any] | None = None,
        feeds: list[dict[str, Any]] | None = None,
    ) -> None:
        self.proactive = dict(proactive or {})
        self.feeds = list(feeds or [])

    @property
    def enabled(self) -> bool:
        return bool(self.proactive.get("enabled", False))


class CuratedFeedsPlugin(Plugin):
    name = "curated-feeds"
    version = "0.3.0"
    desc = "订阅市场、跨公司新模型、arXiv、日本 AI/机器人与 X 时间线"
    ConfigModel = CuratedFeedsConfig

    @classmethod
    def drift_skill_roots(cls) -> tuple[str, ...]:
        return ("drift-skills",)

    @classmethod
    def mcp_servers(cls) -> List[McpServerSpec]:
        return [
            McpServerSpec(
                name="curated_feeds",
                command=("python3", "mcp_server.py"),
            )
        ]

    def proactive_sources(self) -> List[ProactiveSourceSpec]:
        if not self.context.config.enabled:
            return []
        return [
            ProactiveSourceSpec(
                id="subscriptions",
                channels=("content",),
                server="curated_feeds",
                fetch_tool="get_proactive_events",
                ack_tool="ack_proactive_events",
            )
        ]

    def static_semantic_checks(self) -> List[PluginSemanticCheck]:
        config = self.context.config
        if not config.enabled:
            return [PluginSemanticCheck.ok("subscriptions_disabled")]
        if not config.feeds:
            return [
                PluginSemanticCheck.fail(
                    "subscriptions_configured", "enabled=true 但 feeds 为空"
                )
            ]
        ids = [str(item.get("id") or "").strip() for item in config.feeds]
        if any(not item for item in ids) or len(ids) != len(set(ids)):
            return [
                PluginSemanticCheck.fail(
                    "subscription_ids", "feed id 必须非空且唯一"
                )
            ]
        return [
            PluginSemanticCheck.ok(
                "subscriptions_configured", "%d sources" % len(config.feeds)
            )
        ]
