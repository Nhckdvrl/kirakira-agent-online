# 当前状态

更新日期：2026-08-12。

Kirakira 的正式产品入口已经收敛为 Cloud API 与 Cloud worker，不提供 Local mode。旧 SQLite、TUI、
MessageBus 和 host execution 源码只作为测试/算法回归 adapter，`kirakira-agent` 不再安装为公开命令。

## 已完成

- 多用户认证、Conversation/Message/Run durable 数据模型与跨租户隔离。
- 异步 worker、数据库 lease/heartbeat/reaper、取消、幂等提交、同会话串行和跨会话并行。
- durable event、SSE、流式 delta、tool timeline、限流、安全响应头、readiness、metrics、JSON 日志。
- 原 ReAct、context、compaction、Default Memory、Proactive 和 Drift 算法的 Cloud composition。
- PostgreSQL Memory/profile、pgvector candidate recall、工具 checkpoint、remote sandbox fail-closed 合同。
- durable 主动 Agent conversation、外部事件 inbox、automation lease/heartbeat、幂等投递与被动优先 fencing。
- systemd API/多 worker 部署单元，以及 SQLite 快速测试和真实 PostgreSQL 集成测试。
- Neon main 已执行完整 Alembic `20260809_03 → 20260809_13` migration；临时验证分支已清理。

## 代码外的部署条件

- 在目标环境部署满足能力证明合同的 isolated sandbox 服务。
- 配置 PostgreSQL、主模型和 1024 维 embedding endpoint 的 secret。
- 安装 systemd units，并按目标模型延迟与配额执行容量压测。

这些是目标运行环境和第三方服务的接入工作，不是 Local fallback：缺少 PostgreSQL、模型、1024 维
embedding 或隔离 sandbox 时，Cloud worker 会拒绝启动，不会静默降级算法或在宿主机执行工具。

完整方案、实现说明、验证数字和仍需产品决定的费用/保留策略统一见
[Cloud 总方案](../cloud-engineering/plan.md)。
