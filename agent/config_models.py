"""Typed configuration projection consumed by memory engines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MemoryEmbeddingConfig:
    # 默认值照 Reference agent/config_models.py:85——只配 base_url 不配 model 时,
    # Embedder 不应带着空 model 发请求(doctor 的 embedding_configured 判据同此口径)。
    model: str = "text-embedding-v3"
    api_key: str = ""
    base_url: str = ""
    output_dimensionality: int | None = None


@dataclass
class MemoryConfig:
    enabled: bool = True
    # 注意与 kirakira 自己的 `[memory].engine` 区分:那个是 M1 迁移的**存储 owner**
    # 选择器(auto/legacy/coremem,由旧 MemoryRuntime 消费);这里的 plugin 才是
    # Reference 语义上的**引擎插件名**(default/akasha)。两者语义不同,不能合并——
    # 合并会让老 workspace 的 `engine="auto"` 被当成引擎名解析失败。
    plugin: str = "default"
    engine: str = "default"
    embedding: MemoryEmbeddingConfig = field(default_factory=MemoryEmbeddingConfig)


@dataclass
class Config:
    model: str
    api_key: str = ""
    base_url: str | None = None
    light_model: str = ""
    light_api_key: str = ""
    light_base_url: str = ""
    memory: MemoryConfig = field(default_factory=MemoryConfig)


def build_config(app_config: dict[str, Any]) -> Config:
    """从 kirakira 的 config.toml dict 构建引擎所需的 Config。

    kirakira 只有单一主模型,没有独立 light 模型,故 light_* 回退到主模型。
    差异隔离在此适配器内,引擎照抄 Reference 不动。
    """

    from agent.config import config_value

    model = str(config_value(app_config, "llm", "main", "model", default=""))
    base_url = str(config_value(app_config, "llm", "main", "base_url", default=""))
    api_key = str(config_value(app_config, "llm", "main", "api_key", default=""))

    light_model = str(
        config_value(app_config, "llm", "light", "model", default="") or model
    )
    light_base_url = str(
        config_value(app_config, "llm", "light", "base_url", default="") or base_url
    )
    light_api_key = str(
        config_value(app_config, "llm", "light", "api_key", default="") or api_key
    )

    embed_dim_raw = config_value(
        app_config, "memory", "embedding", "output_dimensionality", default=None
    )
    embedding = MemoryEmbeddingConfig(
        model=str(
            config_value(app_config, "memory", "embedding", "model", default="")
            or "text-embedding-v3"
        ),
        api_key=str(config_value(app_config, "memory", "embedding", "api_key", default="")),
        base_url=str(config_value(app_config, "memory", "embedding", "base_url", default="")),
        output_dimensionality=int(embed_dim_raw)
        if embed_dim_raw not in (None, "")
        else None,
    )
    memory = MemoryConfig(
        enabled=bool(config_value(app_config, "memory", "enabled", default=True)),
        plugin=str(
            config_value(app_config, "memory", "plugin", default="") or "default"
        ),
        engine=str(config_value(app_config, "memory", "engine", default="default")),
        embedding=embedding,
    )
    return Config(
        model=model,
        api_key=api_key,
        base_url=base_url,
        light_model=light_model,
        light_api_key=light_api_key,
        light_base_url=light_base_url,
        memory=memory,
    )
