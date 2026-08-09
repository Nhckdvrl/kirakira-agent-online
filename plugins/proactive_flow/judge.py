"""主动推送的 LLM 判断层。

给定候选事件 + 长期记忆 + 近期对话 + 背景 context，让模型决定：
- alert：必须自然化成一句人话推送（不问该不该发，只问怎么说）
- content：先做兴趣判断，再决定 send / skip

参考 akashic 的 `plugins/wake_proactive/prompt.py` + `tools.py`。kirakira 的
``ModelClient.complete`` 不支持 tool_choice，所以这里让模型直接产出严格 JSON，
再解析——同样是"LLM 决策"，且不依赖具体 provider 的 forced-tool 能力。
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import List

from agent.model_runtime.types import ModelClient
from proactive_v2.contracts import (
    AlertContract,
    ContentContract,
    ContextContract,
)

logger = logging.getLogger(__name__)

_JSON_BLOCK = re.compile(r"\{.*\}", re.S)

_SYSTEM = (
    "你是 Kirakira 的主动推送判断器。你不是在回答用户，而是在判断此刻要不要主动"
    "给用户发一条消息，以及发什么。只输出一个 JSON 对象，不要任何解释或代码块围栏。"
)


@dataclass(slots=True)
class Decision:
    send: bool
    message: str
    cited_ids: List[str]


def _parse(text: str) -> dict:
    match = _JSON_BLOCK.search(text or "")
    if match is None:
        raise ValueError("judge 未返回 JSON: %r" % (text or "")[:200])
    return json.loads(match.group(0))


class ProactiveJudge:
    def __init__(
        self,
        model_client: ModelClient,
        *,
        model: str,
        max_tokens: int,
    ) -> None:
        self._client = model_client
        self._model = model
        self._max_tokens = max_tokens

    async def _complete(self, prompt: str) -> str:
        """在线程里跑同步的 complete，避免阻塞事件循环。"""
        response = await asyncio.to_thread(
            self._client.complete,
            messages=[{"role": "user", "content": prompt}],
            tools=[],
            system=_SYSTEM,
            model=self._model,
            max_tokens=self._max_tokens,
        )
        return response.text or ""

    async def decide_alert(
        self,
        alert: AlertContract,
        *,
        memory_text: str,
        recent_conversation: str,
        proactive_context: str,
        current_context: str,
        recent_proactive: str = "",
    ) -> Decision:
        prompt = _build_prompt(
            channel="alert",
            candidates=alert.to_prompt_line(),
            memory_text=memory_text,
            recent_conversation=recent_conversation,
            proactive_context=proactive_context,
            current_context=current_context,
            recent_proactive=recent_proactive,
            instruction=(
                "这是一条高优先级 alert，必须推送。请把它自然化成一句符合 Kirakira 语气的话。"
                '输出：{"message": "要发送的话"}'
            ),
        )
        # alert 是高优先级：模型/解析失败也要发（回退原文），不能因一次异常吞掉告警。
        try:
            data = _parse(await self._complete(prompt))
        except Exception:
            logger.exception("[proactive.judge] alert 判断失败，回退原文发送")
            data = {}
        message = str(data.get("message") or alert.title or alert.content).strip()
        return Decision(send=True, message=message, cited_ids=[alert.item_id])

    async def decide_content(
        self,
        contents: List[ContentContract],
        *,
        memory_text: str,
        recent_conversation: str,
        proactive_context: str,
        current_context: str,
        recent_proactive: str = "",
    ) -> Decision:
        lines = "\n".join(
            item.to_prompt_line(index) for index, item in enumerate(contents)
        )
        prompt = _build_prompt(
            channel="content",
            candidates=lines,
            memory_text=memory_text,
            recent_conversation=recent_conversation,
            proactive_context=proactive_context,
            current_context=current_context,
            recent_proactive=recent_proactive,
            instruction=(
                "判断这些内容里有没有值得此刻主动打扰用户去分享的。宁缺毋滥：不确定就 skip。"
                "如果值得分享，选一到两条，写一条自然的推送消息，并列出引用的 id。\n"
                '输出：{"decision": "send" 或 "skip", "message": "要发送的话（skip 时空字符串）",'
                ' "cited_ids": ["被引用的候选 id"]}'
            ),
        )
        # content 是可选内容：模型/解析失败时默认 skip（宁缺毋滥），不打扰用户。
        try:
            data = _parse(await self._complete(prompt))
        except Exception:
            logger.exception("[proactive.judge] content 判断失败，默认 skip")
            return Decision(send=False, message="", cited_ids=[])
        send = str(data.get("decision") or "").strip().lower() == "send"
        message = str(data.get("message") or "").strip()
        valid_ids = {item.item_id for item in contents}
        cited = [str(i) for i in (data.get("cited_ids") or []) if str(i) in valid_ids]
        if send and (not message or not cited):
            # 声称要发却没内容或没引用，视为无效决策，安全起见 skip。
            return Decision(send=False, message="", cited_ids=[])
        return Decision(send=send, message=message, cited_ids=cited)


def _build_prompt(
    *,
    channel: str,
    candidates: str,
    memory_text: str,
    recent_conversation: str,
    proactive_context: str,
    current_context: str,
    instruction: str,
    recent_proactive: str = "",
) -> str:
    sections = [
        f"【通道】{channel}",
        "【长期记忆】\n" + (memory_text.strip() or "（空）"),
        "【近期对话】\n" + (recent_conversation.strip() or "（无）"),
        "【最近已推送的主动消息（避免重复）】\n" + (recent_proactive.strip() or "（无）"),
        "【主动推送规则】\n" + (proactive_context.strip() or "（无额外规则）"),
        "【当前背景 context】\n" + (current_context.strip() or "（无）"),
        "【候选事件】\n" + (candidates.strip() or "（无）"),
        "【任务】\n" + instruction,
    ]
    return "\n\n".join(sections)


def format_context(contexts: List[ContextContract]) -> str:
    if not contexts:
        return ""
    return "\n".join(item.to_prompt_line() for item in contexts)
