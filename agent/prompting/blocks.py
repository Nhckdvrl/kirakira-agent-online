"""Prompt blocks with stable names, priorities, and optional static caching."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import platform
from typing import Protocol

from agent.prompting.assembler import (
    PromptSectionMeta,
    PromptSectionRender,
    SectionCache,
)


@dataclass
class TurnContext:
    workspace: Path
    memory: object
    skills: object
    system_prompt: str
    behavior_rules: str
    skill_names: list[str]
    channel: str
    chat_id: str
    retrieved_memory_block: str
    turn_injection_prompt: str
    extra_hints: list[str]


class PromptBlock(Protocol):
    priority: int
    label: str
    is_static: bool

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None: ...

    def cache_signature(self, ctx: TurnContext) -> str | None: ...


class _Block:
    priority = 0
    label = ""
    is_static = False

    def cache_signature(self, ctx: TurnContext) -> str | None:
        return None


class IdentityBlock(_Block):
    priority = 10
    label = "identity"
    is_static = True

    def render(self, ctx: TurnContext, signature: str | None = None) -> str:
        root = str(ctx.workspace.resolve())
        return "# Kirakira Agent\n\n%s\n\n## 工作区\n- 根目录：%s" % (
            ctx.system_prompt,
            root,
        )

    def cache_signature(self, ctx: TurnContext) -> str:
        return "%s:%s" % (ctx.workspace.resolve(), ctx.system_prompt)


class BehaviorRulesBlock(_Block):
    priority = 15
    label = "behavior_rules"
    is_static = True

    def render(self, ctx: TurnContext, signature: str | None = None) -> str:
        return "## 行为规范\n" + ctx.behavior_rules

    def cache_signature(self, ctx: TurnContext) -> str:
        return ctx.behavior_rules


class SkillsCatalogBlock(_Block):
    priority = 20
    label = "skills_catalog"
    is_static = True

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None:
        catalog = signature or ""
        return "## Skills\n%s" % catalog if catalog and catalog != "(no skills)" else None

    def cache_signature(self, ctx: TurnContext) -> str | None:
        catalog = ctx.skills.descriptions()
        return catalog or None


class SelfModelBlock(_Block):
    priority = 30
    label = "self_model"

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None:
        value = ctx.memory.store.read_self().strip()
        return "## Self Model\n%s" % value if value else None


class LongTermMemoryBlock(_Block):
    priority = 35
    label = "long_term_memory"

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None:
        value = ctx.memory.store.read_long_term().strip()
        return "## Long-Term Memory\n%s" % value if value else None


class SessionContextBlock(_Block):
    priority = 40
    label = "session_context"

    def render(self, ctx: TurnContext, signature: str | None = None) -> str:
        return "## Current Session\nChannel: %s\nChat ID: %s\nMachine: %s" % (
            ctx.channel,
            ctx.chat_id,
            platform.machine(),
        )


class RecentContextBlock(_Block):
    priority = 45
    label = "recent_context"

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None:
        value = ctx.memory.store.read_recent_context().strip()
        marker = "\n## Recent Turns"
        cut = value.find(marker)
        value = value[:cut].strip() if cut != -1 else value
        return value or None


class ActiveSkillsBlock(_Block):
    priority = 50
    label = "active_skills"

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None:
        names = [*ctx.skills.always_names(), *ctx.skill_names]
        if not names:
            return None
        loaded = [ctx.skills.load(name) for name in dict.fromkeys(names)]
        return "\n\n".join(item for item in loaded if not item.startswith("Error:")) or None


class RetrievedMemoryBlock(_Block):
    priority = 55
    label = "retrieved_memory"

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None:
        return ctx.retrieved_memory_block.strip() or None


class TurnInjectionBlock(_Block):
    priority = 60
    label = "turn_injection"

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None:
        return ctx.turn_injection_prompt.strip() or None


class PluginHintsBlock(_Block):
    priority = 65
    label = "plugin_hints"

    def render(self, ctx: TurnContext, signature: str | None = None) -> str | None:
        return "\n".join(ctx.extra_hints).strip() or None


class SystemPromptBuilder:
    def __init__(self, blocks: list[PromptBlock], cache: SectionCache | None = None):
        self._blocks = sorted(blocks, key=lambda block: block.priority)
        self._cache = cache or SectionCache()

    def build(
        self,
        ctx: TurnContext,
        *,
        disabled_sections: set[str] | None = None,
    ) -> tuple[list[PromptSectionRender], list[PromptSectionMeta]]:
        disabled = disabled_sections or set()
        scope = str(ctx.workspace.resolve())
        renders: list[PromptSectionRender] = []
        metas: list[PromptSectionMeta] = []
        for block in self._blocks:
            if block.label in disabled:
                continue
            signature = block.cache_signature(ctx) if block.is_static else None
            rendered = None
            cache_hit = False
            if signature:
                rendered = self._cache.get(scope, block.label, signature)
                cache_hit = rendered is not None
            if rendered is None:
                rendered = block.render(ctx, signature)
                if rendered and signature:
                    self._cache.set(scope, block.label, signature, rendered)
            if not rendered:
                continue
            section = PromptSectionRender(block.label, rendered, block.is_static, cache_hit)
            renders.append(section)
            metas.append(
                PromptSectionMeta(
                    block.label,
                    len(rendered),
                    max(1, len(rendered) // 3),
                    block.is_static,
                    cache_hit,
                )
            )
        return renders, metas


def default_prompt_blocks() -> list[PromptBlock]:
    return [
        IdentityBlock(),
        BehaviorRulesBlock(),
        SkillsCatalogBlock(),
        SelfModelBlock(),
        LongTermMemoryBlock(),
        SessionContextBlock(),
        RecentContextBlock(),
        ActiveSkillsBlock(),
        RetrievedMemoryBlock(),
        TurnInjectionBlock(),
        PluginHintsBlock(),
    ]
