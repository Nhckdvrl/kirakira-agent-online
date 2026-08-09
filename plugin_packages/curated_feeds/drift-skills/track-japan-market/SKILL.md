---
name: track-japan-market
description: 跟踪日经225与日本交易所信息；指数显著波动或 X 官方账号出现新线索时选择
---

# 跟踪日本市场

## 目标

每轮将一个日经225行情信号与一条日经或 JPX 来源联系起来，留下可验证的市场观察。

## 单次闭环

1. 读取 Drift Briefing 与 journal，选一个未处理的交易日/波动档位。
2. 查看日经225变动，再用 `web_fetch` 核对日经、JPX 或其他一手来源；X 帖子只是线索，不自动当成原因。
3. 写出已知事实、候选解释和下一个可观察的验证点。
4. 用 `journal_append(entry_type="progress", ...)` 记录，key 用交易日+波动档位。
5. 只有高置信、高相关的新信号才 `message_push`；最多一次。
6. 调用 `finish_drift`，briefing 包含数据时间和来源。

## 约束

- 不把 X 上的单一观点写成已证实的市场因果。
- 不给出个股交易建议。
- 未完成时用 `paused + scratchpad_update` 保存继续点。
