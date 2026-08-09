---
name: track-a-share-market
description: 跟踪 A 股宽基指数的显著波动与驱动因素；上证、沪深300或创业板出现新信号时选择
---

# 跟踪 A 股市场

## 目标

每轮只解释一个 A 股宽基指数的显著波动，积累“行情—驱动因素—后续验证”记录，不生成个股交易指令。

## 单次闭环

1. 阅读 Drift Briefing 与 journal，避免重复解释同一个交易日、同一档波动。
2. 从上证综指、沪深300、创业板指中选一个最显著的信号，用 `web_search` / `web_fetch` 查找至少一个可回溯来源。
3. 区分已知事实与推测，形成一条简短解释和一个后续验证点。
4. 用 `journal_append(entry_type="progress", ...)` 记录，key 用交易日+指数+波动档位。
5. 只有驱动因素高置信且对当前市场观察明显有用时，才 `message_push` 一次；否则静默收尾。
6. 必须调用 `finish_drift`；未完成时用 `paused + scratchpad_update`。

## 约束

- 不给出买入、卖出、仓位或收益承诺。
- 明确标记行情数据时间，不把延迟报价写成实时。
- `message_push` 最多一次，发送后立即收尾。
