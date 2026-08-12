# Kirakira Cloud Agent

Kirakira 是一个可上线的多用户 Agent 应用后端。浏览器通过 API 提交消息，PostgreSQL 保存按用户隔离的
Conversation、Message、Run、Memory 与后台任务状态，worker 异步执行原有 Agent 算法，文件和代码工具
只能进入经过能力证明的远程隔离 sandbox。

[English](./README.md) · [文档索引](./docs/INDEX.md) ·
[Cloud 总方案与实施记录](./docs/cloud-engineering/plan.md)

## 已经完成

- opaque cookie 认证、跨用户隔离、有界列表查询、账号/会话删除、Origin 防护、数据库限流和安全响应头。
- 用户消息与 queued Run 同事务写入；worker 使用 PostgreSQL lease、heartbeat、取消、过期恢复、
  同会话串行和幂等提交。
- durable Run event 与可断点续接的 SSE 文本/tool timeline。
- 原有 ReAct、上下文投影、compaction、Default Memory 评分、Proactive energy/gate/judge、
  Drift hazard/公平排序/journal 算法原样复用。
- 按用户隔离的 PostgreSQL Memory/profile。pgvector/HNSW 只做候选召回，最终排序仍由原数学公式完成。
- durable 主动 Agent 调度：Proactive 消费 API/webhook inbox 事件；没有适合推送的内容时进入 Drift。
  用户只需选择一个主动 Agent 会话作为投递目标，实际 tick 由后台 worker 自动触发。
- durable Scheduler（at/after/every/cron/timezone/misfire）、文件上传下载、多模态附件与完整浏览器界面。
- Telegram、OneBot QQ、腾讯 QQBot 安全配对、入站去重、durable outbox 和主动消息投递。
- 按用户隔离的远程 MCP、Cloud Plugin、Skill 与 durable 子 Agent；每轮 snapshot lease 防止能力串租户或
  中途换代。
- Bubblewrap sandbox 能力证明、shell/PTY/文件隔离、工具 checkpoint、Prometheus、JSON 日志、readiness
  和四类生产进程的 systemd unit。

Local TUI/setup/bootstrap 已不再是产品模式，也不再安装公开命令。源码中的少量旧 adapter 仅作为算法和
合同回归夹具保留。

## 启动在线服务

要求 Python 3.11+、Bubblewrap、带 pgvector 的 PostgreSQL，以及 OpenAI-compatible 对话/embedding 服务。

```bash
cp .env.example .env
cp config.example.toml config.toml
uv sync --group dev
uv run alembic upgrade head
uv run kirakira-sandbox
uv run kirakira-cloud-api
uv run kirakira-cloud-worker
uv run kirakira-channel-gateway  # 使用外部渠道时启动
```

生产环境可使用 [`deploy/systemd`](./deploy/systemd) 启动多个 worker。API 不会在 HTTP 请求内执行 Agent；
提交消息后返回 Run ID，客户端通过 SSE 读取 durable event。

主要接口：

```text
POST   /v1/auth/register | /v1/auth/login
GET    /v1/conversations
POST   /v1/conversations
GET    /v1/conversations/{id}/messages
POST   /v1/conversations/{id}/messages
PUT    /v1/conversations/{id}/automation
POST   /v1/conversations/{id}/proactive-events
GET    /v1/schedules | POST /v1/conversations/{id}/schedules
POST   /v1/conversations/{id}/files
POST   /v1/channel-pairings | GET /v1/channel-links
GET    /v1/mcp-servers | POST /v1/mcp-servers
GET    /v1/plugins | POST /v1/plugins
GET    /v1/skills | POST /v1/skills
GET    /v1/subagents
GET    /v1/runs/{id}
POST   /v1/runs/{id}/cancel
GET    /v1/runs/{id}/events/stream
GET    /readyz | /metrics
```

Automation 配置中的开关用于选择主动投递会话和运维控制，并不是手动触发 Proactive/Drift；调度时机始终
由后台算法和 durable scheduler 决定。

## 验证

```bash
uv run pytest -q
KIRAKIRA_TEST_POSTGRES_URL='postgresql://…' \
  uv run pytest -q tests/test_cloud_postgres_integration.py
```

PostgreSQL 集成测试会真实验证 `SKIP LOCKED` 竞争、并发限流、消息序号行锁和 1024 维 pgvector 镜像。
最新证据见[验证记录](./docs/operations/verification.md)，架构、取舍和部署边界集中记录在
[Cloud 总方案](./docs/cloud-engineering/plan.md)。

## 目录

```text
cloud/            API、UI、durable store、worker、渠道/MCP/Plugin/Scheduler/子 Agent
sandbox_service/  Bubblewrap 隔离执行服务
agent/            ReAct、上下文、工具、MCP 与执行合同
core/             Memory 与共享 runtime 合同
memory2/          原始结构化记忆算法
plugins/          Memory、Proactive 与 Drift 实现
proactive_v2/     主动 frame 与编排
deploy/systemd/   API、worker、sandbox、channel gateway 生产 unit
tests/            单测、合同测试、并发与 PostgreSQL 集成测试
```

## License

MIT，见 [LICENSE](./LICENSE)。
