# 0005 Drift 使用 hazard 采样到期

- 状态：accepted

## 背景

只用固定 `min_interval_hours` 会让 Drift 像定时任务。若每次轮询都按概率判定，缩短 tick 间隔又会无意
增加触发次数。

## 决定

根据空闲驱动与内容、近期 Drift、重复度等抑制项计算 hazard，再采样下一次到期时间，持久化到
`drift_schedule(session_key, timer_anchor, next_attempt_at)`。到期只开启一次尝试。

`min_interval_hours` 继续作为硬下限。timer anchor 由最近用户活动和最近 Drift 组成；任一变化都会按新
状态重采样。

## 影响

轮询频率不再改变触发概率，到期时间跨重启保持。一轮 Drift 完成后清除旧排程，再根据新的空闲状态采样。
没有用户活动基准时不额外设卡，仍由最小间隔和其他 gate 决定。
