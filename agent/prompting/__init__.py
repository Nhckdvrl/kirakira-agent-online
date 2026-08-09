"""Structured prompt assembly and context-pressure policies."""

from agent.prompting.assembler import (
    ContextRenderResult,
    PromptAssembler,
    PromptSectionMeta,
    PromptSectionRender,
    SYSTEM_CONTEXT_FRAME_MARKER,
    build_context_frame_content,
    build_context_frame_message,
    is_context_frame,
)
from agent.prompting.budget import (
    ContextTrimPlan,
    DEFAULT_CONTEXT_TRIM_PLANS,
)

__all__ = [
    "ContextRenderResult",
    "ContextTrimPlan",
    "DEFAULT_CONTEXT_TRIM_PLANS",
    "PromptAssembler",
    "PromptSectionMeta",
    "PromptSectionRender",
    "SYSTEM_CONTEXT_FRAME_MARKER",
    "build_context_frame_content",
    "build_context_frame_message",
    "is_context_frame",
]
