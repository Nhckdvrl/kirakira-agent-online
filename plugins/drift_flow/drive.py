"""Drift hazard drive(照 Reference `plugins/wake_proactive/drift_drive.py`)。

kirakira 原本用固定 `min_interval_hours` 门控:满 N 小时就允许跑一轮。这在体感上是
"定时打卡",而不是"闲下来了才去做点事"。

Reference 的模型把它变成连续量:
- **idle_drive**:用户越久没说话,越该利用空闲(`1 - e^(-idle/4h)`);
- **抑制项**:本轮有内容要推(content)、刚跑过 Drift(recent_drift)、最近在重复
  (repetition),三者都压低速率;
- **hazard**:速率对时间积分并按 12 小时半衰期衰减。

关键设计(Reference 注释原话):**到期事件只负责开启一次判别,不再依赖轮询 hazard 穿线**。
所以真正驱动触发的是 `sample_drift_delay_hours`——从 hazard 分布里**采样**下一次到期时刻,
存下来等它到期。轮询判阈会让"检查得越频繁越容易触发"这种采样假象混进来,采样到期没有这个问题。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Tuple

DriftDecision = Literal["attempt", "idle"]

_HAZARD_HALF_LIFE_HOURS = 12.0
_IDLE_TIME_CONSTANT_HOURS = 4.0


def _bounded(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


@dataclass(frozen=True)
class DriftDriveResult:
    decision: DriftDecision
    hazard_before: float
    hazard_after: float
    threshold: float
    rate: float
    idle_hours: float
    idle_drive: float
    content_suppression: float
    recent_drift_suppression: float
    repetition_suppression: float
    reasons: Tuple[str, ...]


def advance_drift_drive(
    *,
    now: datetime,
    hazard: float,
    threshold: float,
    updated_at: datetime | None,
    last_user_at: datetime | None,
    last_drift_at: datetime | None,
    content_evidence: float,
    repetition: float = 0.0,
    max_rate_per_hour: float = 0.3,
) -> DriftDriveResult:
    """把上次 hazard 推进到现在,并给出是否该尝试的判断与可读理由。"""
    content = _bounded(content_evidence)
    repetition_score = _bounded(repetition)
    idle_hours = (
        max(0.0, (now - last_user_at).total_seconds() / 3600)
        if last_user_at is not None
        else 0.0
    )
    idle_drive = 1.0 - math.exp(-idle_hours / _IDLE_TIME_CONSTANT_HOURS)
    recent_drift_suppression = (
        math.exp(-max(0.0, (now - last_drift_at).total_seconds()) / (6 * 3600))
        if last_drift_at is not None
        else 0.0
    )
    rate = (
        max_rate_per_hour
        * idle_drive
        * (1.0 - 0.95 * content)
        * (1.0 - 0.9 * recent_drift_suppression)
        * (1.0 - 0.9 * repetition_score)
    )
    elapsed_hours = (
        max(0.0, (now - updated_at).total_seconds() / 3600)
        if updated_at is not None
        else 5 / 60
    )
    before = max(0.0, hazard)
    time_constant = _HAZARD_HALF_LIFE_HOURS / math.log(2.0)
    retention = math.exp(-elapsed_hours / time_constant)
    after = before * retention + max(0.0, rate) * time_constant * (1.0 - retention)
    attempt = after >= threshold
    return DriftDriveResult(
        decision="attempt" if attempt else "idle",
        hazard_before=before,
        hazard_after=after,
        threshold=threshold,
        rate=rate,
        idle_hours=idle_hours,
        idle_drive=idle_drive,
        content_suppression=content,
        recent_drift_suppression=recent_drift_suppression,
        repetition_suppression=repetition_score,
        reasons=_reasons(
            content=content,
            recent_drift=recent_drift_suppression,
            repetition=repetition_score,
            attempt=attempt,
        ),
    )


def sample_drift_delay_hours(
    *,
    random_draw: float,
    idle_hours: float,
    recent_drift_suppression: float,
    repetition_suppression: float,
    max_rate_per_hour: float = 0.08,
) -> float:
    """从递增的空闲 hazard 采样下一次 Drift 到期时间(小时)。

    做法是解 `∫rate dt = -ln(1-u)`:把均匀随机数映射成累计 hazard 目标量,再单调求解
    需要多久才能积到这个量。抑制项越强,积得越慢,到期越晚。
    """
    scale = (
        max_rate_per_hour
        * (1.0 - 0.9 * _bounded(recent_drift_suppression))
        * (1.0 - 0.9 * _bounded(repetition_suppression))
    )
    target = -math.log1p(-min(1.0 - 1e-12, max(0.0, random_draw)))
    start = max(0.0, idle_hours)
    start_mass = _integrated_idle_drive(start, scale)

    # 抑制项拉到极致时 scale 可能为 0,此时永远积不到目标:直接给一个很远的到期,
    # 让调用方按"本轮不跑"处理,而不是在下面的倍增里死循环。
    if scale <= 0.0 or target <= 0.0:
        return float("inf")

    low = start
    high = low + 1.0
    for _ in range(64):
        if _integrated_idle_drive(high, scale) - start_mass >= target:
            break
        high = low + 2.0 * (high - low)
    for _ in range(64):
        middle = (low + high) / 2.0
        if _integrated_idle_drive(middle, scale) - start_mass < target:
            low = middle
        else:
            high = middle
    return high - start


def _integrated_idle_drive(idle_hours: float, scale: float) -> float:
    return scale * (
        idle_hours
        - _IDLE_TIME_CONSTANT_HOURS
        * (1.0 - math.exp(-idle_hours / _IDLE_TIME_CONSTANT_HOURS))
    )


def _reasons(
    *,
    content: float,
    recent_drift: float,
    repetition: float,
    attempt: bool,
) -> Tuple[str, ...]:
    reasons: list[str] = []
    if content >= 0.5:
        reasons.append("content_evidence")
    if recent_drift >= 0.5:
        reasons.append("recent_drift")
    if repetition >= 0.5:
        reasons.append("repetition")
    if attempt:
        reasons.append("leisure_ready")
    return tuple(reasons)
