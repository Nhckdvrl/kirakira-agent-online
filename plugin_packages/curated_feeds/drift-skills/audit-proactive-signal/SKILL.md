---
name: audit-proactive-signal
description: 抽查主动推送的新鲜度、重复性和用户相关性；订阅源已运行一段时间、需要发现噪声时选择
---

# 审计主动信号

## 目标

每轮抽查一个近期主动事件或推送，留下一条可用于调整信息源、兴趣阈值或去重的证据。连续多轮应能看出哪些源有价值、哪些源只在制造噪声。

## 单次闭环

1. 从 Drift Briefing 和近期消息中选一个未审计的主动推送；必要时用 `search_messages` 或 `fetch_messages` 回看上下文。
2. 分别判断：是否真的新、是否与旧推送重复、是否符合已知兴趣、是否有可执行价值。
3. 用 `journal_append(entry_type="progress", ...)` 写入一条审计结果，key 使用事件 ID、URL 或消息 source_ref。
4. 本 skill 默认不打扰用户；只记录证据，不自行更改配置或删除状态。
5. 调用 `finish_drift`，briefing 说明抽查对象与结论，`next_tendency` 写下下次应优先抽查的源。

## 约束

- 不调用 `message_push`，这是纯后台质量审计。
- 没有可回溯事件时也要静默闭环，briefing 如实写“暂无可审计对象”。
- 未完成时用 `paused + scratchpad_update` 保存继续点。
