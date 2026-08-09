"""Memory retrieval pipeline: multi-lane recall fused with RRF.

为什么不是"语义分 * 0.75 + 词法分 * 0.25"：那两个分数**尺度不可比**。cosine 大致落在
[-1, 1]，词法重叠率落在 [0, 1]，两者的分布形状还随 query 长度变化；加权相加得到的名次没有
任何统一含义，调权重基本靠玄学。

RRF（Reciprocal Rank Fusion）只用**名次**，不用原始分数：

    score(item) = Σ_lane  weight_lane / (k + rank_in_lane)

因此每条 lane 只需要"自己内部排得对"，跨 lane 不需要可比。k=60 是文献常用值，作用是压平
头部差距，让第 1 名和第 2 名不至于差出数量级。

lane 的分工：
- vector lane   口语化、同义改写、"上次说的那个东西"
- lexical lane  变量名、命令、路径、错误码、精确实体

两条 lane 各有各的盲区，这正是要融合的理由。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple

RRF_K = 60
LEXICAL_RRF_WEIGHT = 0.5
HOTNESS_ALPHA = 0.20
HOTNESS_HALF_LIFE_DAYS = 14.0
INJECT_MAX_CHARS = 1200
INJECT_LINE_MAX = 180


@dataclass
class RetrievalRequest:
    query: str
    limit: int = 5
    memory_types: List[str] = field(default_factory=list)
    since: str = ""
    until: str = ""
    session_key: str = ""
    channel: str = ""
    chat_id: str = ""
    history: List[Dict[str, Any]] = field(default_factory=list)
    session_metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[datetime] = None


@dataclass
class RetrievalTrace:
    lanes: Dict[str, int] = field(default_factory=dict)
    fused: int = 0
    injected: int = 0
    used_vector: bool = False
    truncated: bool = False


@dataclass
class RetrievalResult:
    block: str
    records: List[Any] = field(default_factory=list)
    trace: Optional[RetrievalTrace] = None


class MemoryRetrievalPipeline(Protocol):
    """被动 turn 依赖这个协议而不是具体实现，检索策略才可以整体替换。"""

    def retrieve(self, request: RetrievalRequest) -> RetrievalResult: ...


def rrf_fuse(
    lanes: Sequence[Tuple[str, float, Sequence[Any]]],
    *,
    k: int = RRF_K,
) -> List[Tuple[str, float]]:
    """按各 lane 的名次融合候选，返回 [(record_id, rrf_score)]。

    lanes 是 (lane_name, weight, ranked_records)，ranked_records 必须已按该 lane 自己的
    标准排好序。同分时用 id 兜底，避免 set 遍历顺序导致结果不稳定。
    """

    ranks: Dict[str, Dict[str, int]] = {}
    weights: Dict[str, float] = {}
    for lane_name, weight, records in lanes:
        weights[lane_name] = weight
        lane_rank: Dict[str, int] = {}
        for index, record in enumerate(records):
            record_id = str(record.id)
            if record_id not in lane_rank:
                lane_rank[record_id] = index + 1
        ranks[lane_name] = lane_rank

    scored: List[Tuple[str, float]] = []
    all_ids: set[str] = set()
    for lane_rank in ranks.values():
        all_ids |= set(lane_rank)
    for record_id in all_ids:
        score = 0.0
        for lane_name, lane_rank in ranks.items():
            rank = lane_rank.get(record_id)
            if rank is not None:
                score += weights[lane_name] / (k + rank)
        scored.append((record_id, score))
    scored.sort(key=lambda item: (-item[1], item[0]))
    return scored


def hotness_boost(
    reinforcement: int,
    updated_at: Optional[datetime],
    now: datetime,
    *,
    alpha: float = HOTNESS_ALPHA,
    half_life_days: float = HOTNESS_HALF_LIFE_DAYS,
) -> float:
    """强化次数带来的加成随时间半衰，避免陈年旧记忆永远压住新记忆。

    返回一个乘性系数（1.0 表示无加成）。
    """

    if reinforcement <= 0 or updated_at is None:
        return 1.0
    age_days = max(0.0, (now - updated_at).total_seconds() / 86400.0)
    decay = 0.5 ** (age_days / max(1.0, half_life_days))
    strength = math.log1p(reinforcement) / math.log(2.0)
    return 1.0 + alpha * strength * decay


def plan_injection(
    records: Sequence[Any],
    *,
    max_chars: int = INJECT_MAX_CHARS,
    line_max: int = INJECT_LINE_MAX,
) -> Tuple[str, int, bool]:
    """把召回结果编排成注入块，并守住字符预算。

    检索质量再好，也不能让记忆块无上限地吃掉上下文——预算是硬约束，不是建议。
    返回 (block, injected_count, truncated)。
    """

    if not records:
        return "", 0, False
    header = "## Retrieved Long-Term Memory"
    lines: List[str] = [header]
    used = len(header)
    injected = 0
    truncated = False
    for record in records:
        source = " source_ref=%s" % record.source_ref if record.source_ref else ""
        line = "- [%s type=%s reinforced=%d]%s %s" % (
            record.id,
            record.memory_type,
            record.reinforcement,
            source,
            record.content,
        )
        if len(line) > line_max:
            line = line[: max(0, line_max - 3)] + "..."
            truncated = True
        if used + len(line) + 1 > max_chars and injected > 0:
            truncated = True
            break
        lines.append(line)
        used += len(line) + 1
        injected += 1
    return "\n".join(lines), injected, truncated
