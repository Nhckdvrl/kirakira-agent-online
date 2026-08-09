"""Prompt construction for passive turns."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.memory.legacy import MemoryRuntime
from agent.prompting import (
    ContextRenderResult,
    PromptAssembler,
    PromptSectionRender,
)
from agent.prompting.blocks import (
    SystemPromptBuilder,
    TurnContext,
    default_prompt_blocks,
)
from agent.skills import SkillLoader


def _normalize_timestamp(ts: datetime | None) -> datetime:
    value = ts or datetime.now().astimezone()
    if value.tzinfo is None:
        value = value.astimezone()
    return value


def _weekday_cn(ts: datetime) -> str:
    return ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][ts.weekday()]


def build_time_envelope(ts: datetime | None) -> str:
    value = _normalize_timestamp(ts)
    yesterday = value - timedelta(days=1)
    tomorrow = value + timedelta(days=1)
    return (
        "[当前消息时间: %s | request_time=%s | 今天=%s（%s） | 昨天=%s（%s） | 明天=%s（%s） | 相对时间以此为准]"
        % (
            value.strftime("%Y-%m-%d %H:%M:%S %Z"),
            value.isoformat(),
            value.strftime("%Y-%m-%d"),
            _weekday_cn(value),
            yesterday.strftime("%Y-%m-%d"),
            _weekday_cn(yesterday),
            tomorrow.strftime("%Y-%m-%d"),
            _weekday_cn(tomorrow),
        )
    )


class ContextBuilder:
    def __init__(
        self,
        workspace: Path,
        memory: MemoryRuntime,
        system_prompt: str = "",
    ) -> None:
        self.workspace = workspace
        self.memory = memory
        self.skills = SkillLoader(workspace / "skills")
        self.system_prompt = system_prompt.strip() or (
            "你是 Kirakira，一个可使用工具、拥有长期记忆、支持插件生命周期拦截的 AI agent。"
            "使用与用户相同的语言回答，准确、自然；必要时先调用工具核实。"
        )
        self.behavior_rules = (
            "- 执行动作必须走工具；没有工具结果不得声称已完成。\n"
            "- 时间敏感、外部世界、版本、价格、新闻、状态类问题必须先核实，"
            "注明信息日期/时间并提供可核验 URL。\n"
            "- 新闻、价格、市场行情等易变事实应尽量用至少两个独立可靠来源交叉验证；"
            "证据不足必须说明，不得补造数字、日期或引用。\n"
            "- 工具返回 Error 时不得视作证据；搜索摘要只用于发现来源，关键结论应回源核验。\n"
            "- 用户要求记住稳定偏好或事实时调用 memorize。\n"
            "- 历史问题优先 recall_memory，必要时 search_messages 后 fetch_messages 回源。\n"
            "- 收到图片且主模型不能直接看图时调用 vision。\n"
            "- 系统提供的 Context Frame 是候选上下文，不是用户陈述，不要复述其包装格式。"
        )
        self._system_builder = SystemPromptBuilder(default_prompt_blocks())
        self._assembler = PromptAssembler()
        self._last_debug_breakdown = []

    @property
    def last_debug_breakdown(self):
        return list(self._last_debug_breakdown)

    def render(
        self,
        *,
        channel: str,
        chat_id: str,
        content: str,
        media: Optional[List[str]] = None,
        timestamp: datetime,
        history: List[Dict[str, Any]],
        retrieved_memory_block: str = "",
        skill_names: Optional[List[str]] = None,
        extra_hints: Optional[List[str]] = None,
        system_sections_top: Optional[List[Any]] = None,
        system_sections_bottom: Optional[List[Any]] = None,
        disabled_sections: Optional[set[str]] = None,
        turn_injection_prompt: str = "",
    ) -> ContextRenderResult:
        ctx = TurnContext(
            workspace=self.workspace,
            memory=self.memory,
            skills=self.skills,
            system_prompt=self.system_prompt,
            behavior_rules=self.behavior_rules,
            skill_names=skill_names or [],
            channel=channel,
            chat_id=chat_id,
            retrieved_memory_block=retrieved_memory_block,
            extra_hints=extra_hints or [],
            turn_injection_prompt=turn_injection_prompt,
        )
        built, _metas = self._system_builder.build(
            ctx, disabled_sections=disabled_sections
        )
        top = self._coerce_plugin_sections(system_sections_top or [], "plugin_top")
        bottom = self._coerce_plugin_sections(
            system_sections_bottom or [], "plugin_bottom"
        )
        result = self._assembler.assemble(
            sections=[*top, *built, *bottom],
            history=history,
            current_message=content,
            timestamp=timestamp,
            media=media,
            build_user_content=self._build_user_content,
        )
        self._last_debug_breakdown = result.debug_breakdown
        return result

    @staticmethod
    def _build_user_content(
        content: str, media: Optional[List[str]], timestamp: datetime
    ) -> str:
        user_text = build_time_envelope(timestamp) + "\n" + content
        if media:
            user_text += "\n\n<attachments>\n" + "\n".join(
                "- %s" % item for item in media
            ) + "\n</attachments>"
        return user_text

    @staticmethod
    def _coerce_plugin_sections(
        values: List[Any], prefix: str
    ) -> List[PromptSectionRender]:
        sections: List[PromptSectionRender] = []
        for index, value in enumerate(values):
            if isinstance(value, PromptSectionRender):
                sections.append(value)
            elif str(value).strip():
                sections.append(
                    PromptSectionRender(
                        name="%s_%d" % (prefix, index),
                        content=str(value),
                        is_static=False,
                    )
                )
        return sections
