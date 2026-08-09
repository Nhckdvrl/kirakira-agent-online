"""主动推送链路（Proactive）。

被动链路负责"用户问、agent 答"；这个包负责"agent 自己找你"：
按电量模型自适应轮询三路数据（alert / content / context），由 LLM 判断
是否推送。没有可推的东西时把控制权交给 Drift 链路（``plugins.drift_flow``）。

边界对齐 akashic-agent 的 `proactive_v2` + `plugins/wake_proactive`：当前包含
模块依赖图、插件代际与能力快照租约、三通道、源反馈和逐 tick/step 轨迹。
兴趣向量与更厚的配额/hazard 策略仍可在现有边界内扩展。
"""

from proactive_v2.config import DriftConfig, ProactiveConfig

__all__ = [
    "DriftConfig",
    "ProactiveConfig",
    "ProactiveLoop",
    "ProactiveSource",
    "SourceRegistry",
    "FileInboxSource",
]


def __getattr__(name: str):
    """Keep package imports acyclic while preserving the public convenience API."""

    if name == "ProactiveLoop":
        from proactive_v2.loop import ProactiveLoop

        return ProactiveLoop
    if name in {"FileInboxSource", "ProactiveSource", "SourceRegistry"}:
        from plugins.wake_proactive import sources

        return getattr(sources, name)
    raise AttributeError(name)
