"""PostResponseMemoryWorker 的模型兼容边界。

Reference 的 `_parse_json_string_array` 要求模型返回**裸 JSON 数组**。这个假设对
Reference 用的模型成立,对 kirakira 默认的 deepseek 不成立:同一段 prompt 下它返回
`{"intent": []}`——用一个单键对象把数组包起来。

后果不是"少一点智能",而是 post-response 的**自动失效检测整条路都抛异常**。真实对话验证时
第一次跑到这条路就炸了(测试全绿也没发现,因为 mock 的 provider 不会这么返回)。

修法刻意放在这里而不是改 `post_response_worker.py`:那个文件在 doctor 的 Reference 漂移
审计范围内,逐字节比对 `Reference/memory2/`。模型兼容属于 kirakira 自己的边界,不该
污染记忆算法源码。

容错只放宽一处:**单键对象且其值是数组**时取出该数组。多键对象、值不是数组、其他类型
一律仍然报错——放宽到"猜模型想说什么"就会把真正的坏响应吞掉。
"""

from __future__ import annotations

import logging
from typing import List

from memory2.post_response_worker import PostResponseMemoryWorker

logger = logging.getLogger(__name__)


class CompatPostResponseMemoryWorker(PostResponseMemoryWorker):
    """在 Reference 解析语义之上,容忍"单键对象包着数组"这一种模型偏差。"""

    @staticmethod
    def _parse_json_string_array(text: str, response_name: str) -> List[str]:
        try:
            return PostResponseMemoryWorker._parse_json_string_array(text, response_name)
        except ValueError:
            unwrapped = _unwrap_single_key_array(text)
            if unwrapped is None:
                raise
            logger.info(
                "post_response %s: 模型用单键对象包裹数组,已解包", response_name
            )
            return PostResponseMemoryWorker._parse_json_string_array(
                unwrapped, response_name
            )


def _unwrap_single_key_array(text: str) -> str | None:
    """`{"k": [...]}` → `[...]`;其余形状返回 None,交回原有的严格报错。"""
    import json

    import json_repair

    stripped = str(text or "").strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 2 and lines[-1].strip() == "```":
            stripped = "\n".join(lines[1:-1]).strip()
    if not stripped.startswith("{"):
        return None
    try:
        payload = json_repair.loads(stripped)
    except Exception:  # noqa: BLE001 - 解析失败交回原始错误路径
        return None
    if not isinstance(payload, dict) or len(payload) != 1:
        return None
    value = next(iter(payload.values()))
    if not isinstance(value, list):
        return None
    return json.dumps(value, ensure_ascii=False)
