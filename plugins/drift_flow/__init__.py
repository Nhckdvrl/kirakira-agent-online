"""Drift 空闲任务链路。

主动链路（proactive）拉了一圈三通道都没东西可推时，agent 不空转，而是进入
Drift 模式：读用户写的 ``SKILL.md``（分步操作指南）当 system prompt，注入一份
Drift Briefing（记忆 + 近期上下文 + 本 skill 连续性），一步步执行，最后调
``finish_drift`` 收尾。

参考 akashic 的 `plugins/drift_flow` + `plugins/wake_proactive/drift_drive.py`。
运行时复用 kirakira 现有 Agent loop 与工具，并在 ``drift.db`` 保留跨轮连续性、
skill journal/self-observation，以及按空闲 hazard 采样的下一次到期时刻。
"""

from plugins.drift_flow.runner import DriftRunner
from plugins.drift_flow.skills import DriftSkill, discover_skills
from plugins.drift_flow.state import DriftStateStore

__all__ = ["DriftRunner", "DriftSkill", "discover_skills", "DriftStateStore"]
