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
- durable Scheduler（at/after/every/cron/timezone/misfire）、用户文件与多模态附件。
- 浏览器产品界面，以及 Telegram、OneBot QQ、腾讯 QQBot 的配对、入站去重和 durable outbox。
- 按用户隔离的远程 MCP、Cloud Plugin、Skill 和 durable 子 Agent；每轮 runtime snapshot/lease 保证一致性。
- 独立 Bubblewrap sandbox 服务，覆盖 shell、PTY/stdin、文件、二进制内容、超时和 owner cleanup。
- API、worker、sandbox、channel gateway 四个生产进程及 systemd units。
- `661 passed, 1 skipped, 4 subtests passed`，wheel/sdist 构建、JavaScript 语法与 Alembic head 验证通过。
- Neon main 已执行完整 Alembic `20260809_03 → 20260812_20` migration；临时验证分支已清理。

## 代码外的部署条件

- 在目标 Linux 环境安装 Bubblewrap，并为 sandbox 配置独立主机/容器、磁盘配额和 cgroup 上限。
- 配置 PostgreSQL、主模型和 1024 维 embedding endpoint 的 secret。
- 配置渠道/MCP/Plugin 所需凭据、域名/TLS、备份和数据保留策略。
- 安装 systemd units，并按真实模型延迟、配额与目标并发执行容量压测。

这些是目标运行环境和第三方服务的接入工作，不是 Local fallback：缺少 PostgreSQL、模型、1024 维
embedding 或隔离 sandbox 时，Cloud worker 会拒绝启动，不会静默降级算法或在宿主机执行工具。

完整方案、实现说明、验证数字和仍需产品决定的费用/保留策略统一见
[Cloud 总方案](../cloud-engineering/plan.md)。
