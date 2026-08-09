"""Kirakira Agent learning harness module."""

import json
import time
from pathlib import Path
from typing import Callable, List, Optional

from core.schema import JsonDict


def estimate_tokens(messages: List[JsonDict]) -> int:
    return len(json.dumps(messages, ensure_ascii=False, default=str)) // 4


def microcompact(messages: List[JsonDict], keep_tool_results: int = 3) -> None:
    tool_messages = [msg for msg in messages if msg.get("role") == "tool"]
    if len(tool_messages) <= keep_tool_results:
        return
    for msg in tool_messages[:-keep_tool_results]:
        content = msg.get("content")
        if isinstance(content, str) and len(content) > 100:
            msg["content"] = "[cleared by microcompact]"


def compact_messages(
    messages: List[JsonDict],
    transcript_dir: Path,
    summary: Optional[str] = None,
    summarizer: Optional[Callable[[str], str]] = None,
) -> List[JsonDict]:
    transcript_dir.mkdir(parents=True, exist_ok=True)
    transcript_path = transcript_dir / ("transcript_%d.jsonl" % int(time.time()))
    with transcript_path.open("w") as handle:
        for msg in messages:
            handle.write(json.dumps(msg, ensure_ascii=False, default=str) + "\n")

    transcript_text = json.dumps(messages, ensure_ascii=False, default=str)
    if summary is None and summarizer is not None:
        summary = summarizer(transcript_text[-80000:])
    if summary is None:
        summary = "Conversation compressed. See transcript for full prior context."

    return [
        {
            "role": "user",
            "content": "[Compressed. Transcript: %s]\n%s" % (transcript_path, summary),
        }
    ]
