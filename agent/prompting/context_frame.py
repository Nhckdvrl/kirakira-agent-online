"""Reference context-frame detector used by Markdown consolidation."""

LEGACY_CONTEXT_FRAME_MARKER = "[SYSTEM_CONTEXT_FRAME]"


def is_context_frame(content: str) -> bool:
    text = content.lstrip()
    return text.startswith("<system-reminder") or text.startswith(
        LEGACY_CONTEXT_FRAME_MARKER
    )
