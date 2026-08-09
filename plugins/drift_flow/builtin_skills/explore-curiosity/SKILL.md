---
name: explore-curiosity
description: 空闲时像朋友一样，随口问用户一个轻量、自然的生活化问题
---

## 目标

补足用户画像里的生活化空白，一次只问一个轻量、自然的问题。

## 工作流程

1. 读 Drift Briefing 里的长期记忆、近期上下文和本 skill 前情，避免短期重复。
2. 基于这些信息，现场想一个轻量、自然、像朋友随口一问的问题。
3. 如果此刻适合聊天，用 `message_push` 发送这个问题（最多一次）。
4. 如果不适合打扰，就不发，直接 `finish_drift(status="completed")` 静默收尾。

## 要求

- 问题要轻量、自然，避开长期记忆里已经明确有答案的信息。
- 不要问太大、太虚、太像采访的问题。
- 结束前必须调用 `finish_drift`，填写 status 与 briefing。
