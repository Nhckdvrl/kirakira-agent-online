# 记忆架构

Kirakira 把“会话事实”“上下文投影”“结构化长期记忆”和“人可编辑 Markdown 记忆”分开管理。它们可以
协作，但不能互相冒充权威数据源。

## 数据层

| 数据 | 权威位置 | 作用 |
| --- | --- | --- |
| 完整会话与 usage | `sessions.db` | turn 历史、工具轨迹、token 账本 |
| Markdown 记忆 | `memory/*.md` | 可读、可编辑的身份、偏好、近期上下文 |
| DefaultMemoryEngine | `memory/coremem.db` | 结构化记忆、embedding、检索证据 |
| Akasha v1 | `memory/akasha.db` | 可选的图式记忆与检索 |

Prompt 中看到的消息只是按预算生成的 projection。压缩或裁剪 projection 不会删除 `sessions.db` 中的
原始消息。

## 服务边界

`core/memory/` 定义 engine seam、事件、Markdown maintenance、embedding 和 store。Agent runtime 只依赖
统一的 MemoryServices，不理解 HyDE、RRF、图遍历等引擎内部算法。

```text
TurnCommitted
  → MarkdownMemoryMaintenance 归档和刷新近期上下文
  → PostResponse worker 提取可持久事实
  → MemoryEngine 写入结构化存储

下一轮
  → query / recall
  → 检索与回源
  → evidence 注入 Context Frame
```

## Default 与 Akasha

`[memory].engine = "default"` 使用 DefaultMemoryEngine；`"akasha"` 使用现有 Akasha v1。两者通过同一
engine protocol 接入，operator 可以切换，但数据不会假装自动互通。

当前 Akasha v1 可用，暂不为了追随新版实现而强制升级。架构已留出独立 adapter、store 和 migration
空间，未来升级不需要改 AgentLoop。

## Consolidation

`MarkdownMemoryMaintenance` 是当前归档驱动方。它订阅已提交 turn，按 session 串行更新 Markdown
窗口，并在下一轮开始前提供可等待的收口点。待归档历史超过保护阈值且无法推进时，turn 会拒绝继续，
避免静默丢历史。

结构化提取失败不会回滚已经提交的聊天；错误进入日志和 trace，后续仍可重试。相关历史决策见
[0003 consolidation 移交](../decisions/0003-consolidation-handover.md)。

## 记忆工具

- `memorize`：显式写入；
- `recall`：检索证据；
- `memory_admin`：查看和维护 memory 状态；
- CLI memory 子命令：初始化、迁移、检查和评测。

用户配置和排查步骤见[记忆手册](../handbook/memory.md)，质量与链路评测见
[记忆评测](../operations/memory-evaluation.md)。

## 不变量

1. Session 是完整历史的事实源；Context Frame 只是本轮投影。
2. 引擎 store 由 MemoryServices 统一持有并关闭，不重复打开同一数据库。
3. 后台提取失败不能改写已提交聊天的结果。
4. 检索结果必须保留 source reference，便于回源和审计。
5. Reference 目录存在与否不影响任何记忆路径。
