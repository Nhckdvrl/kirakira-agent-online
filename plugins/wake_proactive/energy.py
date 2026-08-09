"""动态电量衰减与主动冲动计算。

移植自 akashic-agent 的 `proactive_v2/energy.py`（纯数学，直接照搬）。

核心思路（多时间尺度指数衰减）：
  E(t) = α·exp(-t/τ₁) + β·exp(-t/τ₂) + γ·exp(-t/τ₃)

  τ₁=30min  短时：对话余温
  τ₂=240min 中时：同一天语境
  τ₃=2880min 长时：关系连续性（48h）

贡献函数：
  D_energy  = 1 - energy            互动饥渴度（越久没说话越高）
  D_recent  = log(1+k)/log(1+scale) 对话语境丰富度（近期消息越多越高）

base_score 越高 → next_tick_from_score 给出的间隔越短 → 越快触发。
"""

from __future__ import annotations

import math
import random as _random
from datetime import datetime, timezone


def compute_energy(
    last_user_at: datetime | None,
    now: datetime | None = None,
    *,
    alpha: float = 0.50,
    beta: float = 0.35,
    gamma: float = 0.15,
    tau1_min: float = 30.0,
    tau2_min: float = 240.0,
    tau3_min: float = 2880.0,
) -> float:
    """返回 [0, 1] 的当前电量。从未收到消息则返回 0.0。"""
    if last_user_at is None:
        return 0.0
    now = now or datetime.now(timezone.utc)
    t = max(0.0, (now - last_user_at).total_seconds() / 60.0)
    return (
        alpha * math.exp(-t / tau1_min)
        + beta * math.exp(-t / tau2_min)
        + gamma * math.exp(-t / tau3_min)
    )


def d_energy(energy: float) -> float:
    """互动饥渴度：energy 越低（越久没互动）→ D_energy 越高。范围 [0, 1]。"""
    return 1.0 - max(0.0, min(1.0, energy))


def d_recent(msg_count: int, scale: float = 10.0) -> float:
    """对话语境丰富度：近期消息越多 → D_recent 越高。对数归一化，上限 1.0。"""
    if msg_count <= 0:
        return 0.0
    return min(1.0, math.log1p(max(0, msg_count)) / math.log1p(max(scale, 1.0)))


def base_score(energy: float, recent_msg_count: int) -> float:
    """把互动饥渴度与语境丰富度合成为一个 [0, 1] 的主动冲动分。

    两条贡献取"软或"（1 - (1-a)(1-b)）：任一维度高都会抬高分数，
    但不会超过 1。久没聊（D_energy 高）或刚聊得很热（D_recent 高）都值得更快回来看一眼。
    """
    de = d_energy(energy)
    dr = d_recent(recent_msg_count)
    return 1.0 - (1.0 - de) * (1.0 - dr)


def next_tick_from_score(
    score: float,
    *,
    tick_s1: int = 2400,
    tick_s0: int = 4800,
    tick_jitter: float = 0.3,
    rng: _random.Random | None = None,
) -> int:
    """根据 base_score 返回下一次 tick 的等待秒数（含随机抖动）。

    score 越高 → 间隔越短 → 单位时间内轮询越频繁 → 越快触发。
    """
    base = tick_s1 if score > 0.20 else tick_s0
    if tick_jitter <= 0:
        return base
    r = (rng or _random).uniform(1.0 - tick_jitter, 1.0 + tick_jitter)
    return max(1, int(base * r))
