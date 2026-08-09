"""DefaultMemoryEngine 的检索/注入配置。

忠实移植 Reference `plugins/default_memory/config.py` 的数据类与默认值,
只裁掉 Reference 插件目录体系相关的 config.local.toml 装配(kirakira 无该结构),
db 路径落在 workspace 内的 `memory/coremem.db`。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class RetrievalThresholdsConfig:
    procedure: float = 0.66
    preference: float = 0.5
    event: float = 0.5
    profile: float = 0.5


@dataclass(frozen=True)
class RetrievalInjectConfig:
    max_chars: int = 6000
    forced: int = 3
    procedure_preference: int = 4
    event_profile: int = 4
    line_max: int = 600


@dataclass(frozen=True)
class RetrievalConfig:
    top_k_history: int = 8
    score_threshold: float = 0.45
    relative_delta: float = 0.2
    procedure_guard_enabled: bool = True
    thresholds: RetrievalThresholdsConfig = field(
        default_factory=RetrievalThresholdsConfig
    )
    inject: RetrievalInjectConfig = field(default_factory=RetrievalInjectConfig)


@dataclass(frozen=True)
class DefaultMemoryConfig:
    db_path: str = ""
    retrieval: RetrievalConfig = field(default_factory=RetrievalConfig)


def resolve_memory_db_path(
    *,
    workspace: Path,
    default_config: DefaultMemoryConfig,
) -> Path:
    root = workspace.resolve(strict=False)
    configured = default_config.db_path or "memory/coremem.db"
    path = (root / configured).resolve(strict=False)
    if not path.is_relative_to(root):
        raise ValueError(f"default_memory.db_path 必须位于 workspace 内: {configured}")
    return path
