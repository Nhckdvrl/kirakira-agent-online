# Cloud 架构总览

Kirakira 是异步、多用户的在线 Agent 后端。HTTP 请求只做认证、准入和 durable state 写入，不在 API
进程中执行模型或工具。

```text
Browser / Telegram / OneBot QQ / Tencent QQBot
  → FastAPI: auth, rate limit, conversation, Run admission
  → PostgreSQL: User → Conversation → Message → Run → RunEvent
  → Cloud workers: SKIP LOCKED claim + lease + heartbeat
  → canonical Agent pipeline: context → ReAct → tools → compaction → Memory
  → Bubblewrap sandbox / model / embedding / per-user MCP & plugins
  → transactional assistant Message + terminal Run
  → resumable SSE

Automation worker
  → durable active Agent conversation + inbox + schedule lease
  → Proactive energy/gate/judge
  → no delivery: Drift hazard/skill fairness/journal
  → idempotent assistant Message in the selected conversation
```

## 真相源与协调

PostgreSQL 是所有用户可见状态的唯一 durable truth。conversation 行锁分配消息序号；partial unique
constraint 限制同会话最多一个 running Run；worker 使用 `FOR UPDATE SKIP LOCKED` 横向竞争。Redis 不是
正确性的前置条件，未来只能作为 wakeup/SSE 热路径加速，故障后 Run 必须仍能从 PostgreSQL 重发现。

Run transition 与有序 event 在同一事务提交。文本 delta 会短暂聚合后写入 durable event；SSE 通过
`Last-Event-ID` 恢复。工具调用先写 checkpoint：完成结果可 replay，只有 started 而没有结果的槽位视为
有歧义并终止 Run，避免盲目重复外部副作用。

## 用户与会话隔离

所有 API 查询先绑定已认证 `user_id`。Conversation、Run、event、Memory、profile、Proactive、Drift
状态都有用户谓词或外键。一个用户可以拥有多个普通 Conversation，但只选择一个“主动 Agent
conversation”作为 Proactive/Drift 投递目标；切换目标会开始新的后台连续性 epoch，旧 reservoir 和
journal 不会进入新会话。

被动消息优先：提交用户消息会撤销同会话的在途 automation lease，后台 Agent 的下一次工具调用和最终
投递都会 fail closed。长 tick 自身持续 heartbeat，失去租约的 worker 不得覆盖 durable truth。

## 算法与 adapter 边界

Cloud composition 使用同一个 `PassiveTurnPipeline`、`DefaultReasoner`、`Agent.arun`、MemoryEngine、
Proactive module DAG 和 DriftRunner。允许替换的是身份、存储、调度、投递和执行环境；不允许复制或简化
ReAct、context projection、compaction、Memory 评分、energy、hazard 和 skill 公平排序。

pgvector/HNSW 只在用户 Memory 很大时召回候选，候选仍由原 Python 公式重排。普通规模直接使用精确
评分，避免 ANN 改变语义。

## 执行隔离

Cloud 启动要求独立 `kirakira-sandbox` 的 `/v1/capabilities` 明确证明：`isolated=true`、
`host_execution=false`、`workspace_isolated=true`。文件读写、binary vision、shell、stdin、timeout、
owner cleanup 都通过同一远程 backend；服务使用 Bubblewrap user/mount/pid/network namespace，Cloud
readiness 不接受 host fallback。生产环境还应以独立安全域、cgroup 和磁盘配额限制资源。

用户扩展不把任意 Python 导入共享 worker：MCP 和 Plugin 只连接经 SSRF 校验的公共 HTTPS 服务，凭据
加密保存；Skill 保存声明文本并通过 task-local overlay 注入。每轮 turn 固定 snapshot lease，生命周期
phase、tool、hook、MCP 和 Skill 不会在执行中途换代。

## 进程与运维

- `kirakira-cloud-api`：可多进程，只处理 HTTP/SSE。
- `kirakira-cloud-worker`：轮询 passive Run、automation、Scheduler、Plugin job/source 和子 Agent。
- `kirakira-sandbox`：Bubblewrap 隔离执行与 owner workspace。
- `kirakira-channel-gateway`：Telegram、OneBot QQ 与腾讯 QQBot 渠道收发。
- PostgreSQL + pgvector：durable state、协调、召回。
- model/embedding provider：server-side credential。

API/worker 提供 JSON 日志、request ID、Prometheus、`/healthz`、`/readyz`；生产 unit 位于
`deploy/systemd/`。配置和启动命令见根目录 README。
