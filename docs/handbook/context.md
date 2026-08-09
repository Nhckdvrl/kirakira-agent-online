# 上下文治理

上下文治理的核心是把“完整历史”和“这次发给模型的内容”分开。

## 真相与投影

`sessions.db/messages` 保存完整历史，正常写入为 append-only。模型请求使用可重算投影：

```text
完整 Session
  → 读取当前历史窗口
  → 渲染具名 PromptBlock
  → 加入 Context Frame、当前消息、工具 schema 和图片
  → 本次模型请求
```

context limit 只能让本次投影变小，不能删除或覆盖持久消息。

## Prompt 结构

稳定 section 包括 identity、behavior rules、skills catalog、self model、长期记忆和 session
context。动态 Context Frame 包括 recent context、active skills、retrieved memory、turn injection
和 plugin hints。

动态块明确标记为系统提供的候选上下文，不冒充用户原话。每个 retry 都重新执行 prompt hooks，避免
沿用上一计划的过期产物。

## 预算

```text
input_budget = floor(context_window × effective_context_percent) - max_tokens
```

预算覆盖 system、消息字段、工具 schema、图片和输出预留。当前 token estimate 是近似值，trace
标记 `estimate_quality=approximate`；计费和真实使用量以 provider usage 为准。

## 分级降载

外层 attempt 依次尝试：

1. 完整内容；
2. 去 skills catalog；
3. 去 recent context；
4. 去 long-term memory；
5. 去 retrieved memory；
6. 历史投影缩到 50%；
7. 历史投影缩到 0。

历史切片回退到 user 边界，不从半组 tool call 开始。2026-08-04 的在线验证在 60 条持久消息上
降到 0 条历史投影后成功，原消息 id 和顺序不变。

## ReAct 工具批次压缩

同一请求执行很多工具时，`QueryCompactor` 可以总结已闭合的旧工具批次，用
`context_compact` 协议消息继续。它至少保留最新批次，只压缩完整的 tool-call/tool-result 组。

仍在运行的 Shell 起点会被 pin；只有 `write_stdin`、`task_output` 或 `task_stop` 表明执行结束后，
相关批次才可压缩。压缩摘要也算一次模型请求并计入 usage。

## Usage

每个模型请求都计数，即使 provider 没返回 usage。聚合字段包括 input/output、cached input、
reasoning output、request count 和 covered request count。

`coverage` 有三种：

- `exact`：所有请求都有完整 input/output usage；
- `partial`：只覆盖部分请求或部分字段；
- `unavailable`：provider 没提供可用数据。

缺遥测不能记成 0。

## 去哪里看

每条 assistant 消息的 `context_trace` 保存所有 attempt、section 大小、预算、选中计划、ReAct
估算和 model usage。控制面还会发布 `ContextPrepared` 与 `ContextBudgetUpdated` 事件。

## 不变量

- 检索记忆不冒充用户原话。
- context pressure 不修改持久历史。
- 工具 schema、图片、摘要请求和输出预留都进入预算或 usage。
- provider 已产生可见 streaming delta 后不能切换备用模型。
