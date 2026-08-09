---
name: review-memory
description: 空闲时抽查一条长期记忆是否仍然准确，纯后台记录，不打扰用户
---

## 目标

利用空闲时间做一次轻量的长期记忆自检。这是纯后台任务，不推送消息给用户。

## 工作流程

1. `recall_memory` 读取长期记忆里的若干条目。
2. 从 Drift Briefing 的本 skill 前情里看上次查到哪，避免重复抽同一条。
3. 挑一条判断是否仍然准确、过时或自相矛盾。
4. 把判断结果写进本轮 `finish_drift` 的 briefing。
5. 如果一轮没查完，用 `status="paused"` 保存下一步。

## 要求

- 不调用 `message_push`。
- 结束前必须调用 `finish_drift`。
- 只读判断，不要在本轮直接删改长期记忆。
