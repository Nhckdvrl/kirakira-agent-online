"""Runtime 的依赖注入缝(照 Reference `agent/looping/ports.py`)。

Reference 在这里做了一个值得照抄的区分:**配置参数**与**服务对象**是两类东西,分开放。
配置是值,可以随便拷贝比较;服务是有生命周期的对象,谁持有、谁关闭必须清楚。混在一个
大 config 里会让"换一个实现"变成改一堆调用点。

`MemoryServices` 已先行落在 `coremem/services.py`(它还要带 store/markdown,和记忆包放一起
更内聚),本模块补上其余分组,并从这里统一再导出,调用方只认一个入口。

推广的意义不是多几个 dataclass,而是让 pipeline 只依赖"服务包"这一层:替换 context 或
session 的实现时,不必改 pipeline 调用点,也不必连带改其余子系统的测试。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from core.memory.services import MemoryServices

if TYPE_CHECKING:
    from agent.prompting.context_builder import ContextBuilder
    from agent.model_runtime.types import ModelClient
    from session.ports import TranscriptStore

__all__ = [
    "LLMConfig",
    "MemoryConfig",
    "LLMServices",
    "MemoryServices",
    "SessionServices",
    "ContextServices",
]


# ── 配置(值,不含服务对象)───────────────────────────────────────────────


@dataclass
class LLMConfig:
    model: str = ""
    light_model: str = ""
    max_iterations: int = 10
    max_tokens: int = 8192


@dataclass
class MemoryConfig:
    window: int = 40

    @property
    def keep_count(self) -> int:
        """上下文携带条数,也是 consolidation 后 session 保留条数。

        与 `coremem.services.memory_keep_count` 同一公式;这里保留属性形式是为了
        让持有配置的一方不必再 import 记忆包。
        """
        return max(2, ((max(1, self.window) + 1) // 2) * 2)


# ── 服务对象分组(只放对象,不放配置参数)──────────────────────────────────


@dataclass
class LLMServices:
    """主模型与轻量模型。light 缺省时回退主模型,调用方不必各自判空。"""

    client: "ModelClient"
    light_client: "Optional[ModelClient]" = None

    @property
    def light(self) -> "ModelClient":
        return self.light_client or self.client


@dataclass
class SessionServices:
    transcript_store: "TranscriptStore"
    # 在场状态(最近活跃)目前由主动链路自己从 session 读,预留位不先摆空实现。
    presence: Any = None

    @property
    def session_manager(self) -> "TranscriptStore":
        """Compatibility name while local callers migrate to the port term."""
        return self.transcript_store


@dataclass
class ContextServices:
    """上下文装配。retrieval_pipeline 留空表示检索走记忆引擎。"""

    context: "ContextBuilder"
    retrieval_pipeline: Any = None
