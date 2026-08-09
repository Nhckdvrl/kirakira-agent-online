# Kirakira Cloud 总方案

评审日期：2026-08-08。

最后实施更新：2026-08-09。本文件同时保存方案评审、原始完整方案与当前实施记录；不再维护平行的
“实施记录”文档。

## 结论

原方案的后端主线成立，但产品目标调整为 **Cloud-only**。同一个 Agent core 暂时保留 Local adapter 仅用于开发、测试和迁移，不再把 Local 作为长期用户产品。Cloud 使用
PostgreSQL 保存 durable state，Redis 负责协调与实时路径；HTTP 请求只创建 Run，worker
异步执行；同 conversation 串行、跨 conversation 并行；会执行代码的工具进入 sandbox。

不建议一次性实施全部方案。工程顺序应是：先建立 turn/持久化 ports，并用现有 Local 路径作回归夹具，再做
PostgreSQL + API 的最小纵向切片，然后拆 worker/queue，最后才是 checkpoint、sandbox、对象存储
和 hardening。

## 当前实施结果

Cloud 主链已经承重，不再是最小纵向切片：

- FastAPI 提供 opaque session 认证、Conversation/Message/Run API、Origin 防护、数据库限流、
  有界列表、删除、readiness、metrics 和 resumable SSE。
- PostgreSQL 是 User、Conversation、Message、Run、event、checkpoint、Memory、profile、
  Proactive、Drift 和 automation schedule 的 durable truth。用户消息 + queued Run、assistant 消息 +
  completed Run 均为事务边界。
- 多 worker 通过 `FOR UPDATE SKIP LOCKED`、lease、heartbeat、reaper 和同 conversation partial unique
  constraint 协作。提交支持 `Idempotency-Key`，工具调用有 replay/ambiguity checkpoint。
- 生产 worker 通过唯一 pipeline 工厂调用原有 ReAct、context projection、compaction 和 Memory；
  PostgreSQL/pgvector 只替换持久化和候选召回，最终 Memory 数学评分未改。
- Proactive 与 Drift 已进入 Cloud worker。用户选择一个主动 Agent conversation 作为投递目标，后台
  scheduler 自动执行；两个开关只是运维能力，不是手工触发。外部事件先进入 durable inbox，仍须经过
  原 energy、gate、judge、冷却和去重；无推送时按原 hazard/公平排序进入 Drift。
- automation tick 有数据库 lease/heartbeat；被动消息会撤销在途后台 lease，后续工具和投递 fail
  closed。主动消息用 tick token 幂等写入，切换目标会开启新的连续性 epoch，旧 reservoir/journal
  不进入新 conversation。
- Cloud 工具执行只接受 remote sandbox；启动时必须证明 isolated、no host execution 和 workspace
  isolation，且需要服务 token。生产进程提供 JSON 日志、Prometheus 和 hardened systemd units。

截至本次更新：快速全量回归 `634 passed, 1 skipped, 22 warnings, 4 subtests passed`；Neon 临时分支上的真实
PostgreSQL 测试已验证 12 worker 单 owner claim、30 并发请求精确限流、20 条同会话并发序号、
1024 维 pgvector 写入和余弦查询。主分支 migration 尚待最终人工确认。

仍需在发布环境完成的不是代码降级项，而是外部部署依赖：提供真实 isolated sandbox 服务、填写模型与
embedding credential、执行主库 migration、安装 API/worker systemd unit，并在目标机器做容量压测。

## 已确认，可直接实施

| 项目 | 状态 | 说明 |
| --- | --- | --- |
| Cloud 是唯一正式产品 | 已确认 | Local SQLite/Shell 仅暂作 dev/test adapter，不承诺 Local 产品体验 |
| 产品 conversation 与 channel chat 分离 | 已确认 | Cloud 不再使用 `channel:chat_id` 作为用户身份 |
| `TurnRequest -> TurnResult` | 已确认 | Agent 调用不要求 MessageBus 或渠道投递 |
| PostgreSQL 是 Cloud durable truth | 已确认 | user/conversation/message/run/checkpoint/final result 进入 PG |
| Redis 故障不得丢 durable run | 已确认 | wakeup/cancel/live stream 可降级，run 可由 PG 重发现 |
| Run 与 HTTP request 解耦 | 已确认 | 提交返回 202，执行生命周期属于 worker |
| 同 conversation 严格串行 | 已确认 | 数据库约束兜底，不依赖 Python 进程锁 |
| Cloud V1 不引入 RabbitMQ | 已确认 | 出现独立 broker 需求后通过 transactional outbox 演进 |
| 第一阶段不做 Kubernetes | 已确认 | Docker 条件具备后先用 Compose 验证水平扩容 |
| Cloud V1 不运行任意 Python plugin | 已确认 | 先允许受控内置工具与 remote MCP |
| 核心 Agent 算法不得因 Cloud 化降级 | 已确认 | Drift、Proactive、Memory、ReAct 与 compaction 复用现有实现和行为测试；只替换存储、调度、身份、投递和执行后端 |

## 对原方案的调整

1. “Redis 只负责 Pub/Sub”表述过窄。正确不变量是“Redis 不成为用户/run 的唯一 durable
   truth”。高负载时可以把 queue polling hot path 放到 Redis，再从 PostgreSQL 加载 run
   detail；LangGraph Agent Server 2026 年已采用这条演进路线。
2. 第一阶段不是一次性建齐六个空 port。先落真实调用点上的 `TurnRequest/TurnResult` 和
   `TranscriptStore`；`RunStore/CheckpointStore/ExecutionBackend` 在其第一个真实消费者出现时落地。
3. 当前 `PassiveTurnPipeline.execute()` 内部仍通过本地兼容 envelope 复用旧链路。这是安全的
   过渡态，但不是最终形态；Cloud API 不应看见该 envelope。
4. SSE 适合作为 Browser 的默认 server-to-client 流，但不是所有场景的唯一选择。若以后需要
   远程终端双向 I/O，可为该能力单独使用 WebSocket，不改变 run/event 的 durable 模型。
5. 1000 个在线用户不是 1000 个同时执行的 Agent。容量目标必须用提交吞吐、平均 run 时长、
   worker 并发、queue wait 和 SSE 连接数分别描述。

## 仍需产品确认（进入对应阶段前确认）

### P0：做 Cloud 数据模型前必须确认

- **首要使用场景**：通用个人 Agent、代码 Agent，还是“个人 Agent + 文件/代码执行”二者兼有？
  这会决定 sandbox 是否为 V1 核心，而不是后期增强。
- **模型费用模式**：用户 BYOK、平台统一付费，还是同时支持？这决定 credential、usage ledger、
  quota、账单和滥用防护的优先级。
- **账号范围**：V1 是否只做个人账号？原方案暂定没有 Organization/Team/RBAC，需产品确认后冻结。
- **数据保留与删除**：conversation/message/run/checkpoint/tool result 默认保留多久；用户删除是软删、
  延迟物理删还是立即物理删。

### P1：做 Memory/File 前确认

- Memory 默认是否 user-global；是否允许 conversation-only 或显式关闭记忆。
- 用户文件是 conversation 级还是 user-global；artifact 和原始上传的保留期是否不同。
- Cloud 是否允许用户提交本地目录，或第一版只允许浏览器上传 / Git 仓库拉取。

### P2：做集成与后台任务前确认

- Remote MCP 的租户授权、域名 allowlist 与 credential 归属。
- Scheduler、Telegram、Proactive、Drift 哪些真正属于 Cloud 产品体验；原方案暂定仅 Scheduler/Telegram
  后置，Proactive/Drift 不进 Cloud V1。
- 外部副作用工具的确认 UI、审批超时与恢复语义。


## Local mode 去留决定

### 决定

- **去除 Local 的产品定位**：README、路线图和新功能不再以本地 TUI/自带 API key 运行作为最终交付目标。
- **最终仓库是纯在线版**：最终部署和公开入口只包含 Cloud API、worker、scheduler、PostgreSQL/Redis adapter、对象存储和 sandbox；不提供 Local mode。
- **迁移期暂不物理删除 Local adapter**：SQLite SessionManager、MessageBus、本机 Shell 和最小调试入口只承担开发、回归和 Cloud adapter 的对照实现。
- **Cloud 纵向切片承重后全部删除**：当 PostgreSQL conversation/run store、API/worker 与 sandbox execution 分别通过合同测试后，逐项删除被替代实现，而不是长期维护双模式。

### 为什么不现在删除

当前本地 bootstrap、TUI/渠道、MessageBus、SQLite session 和本机 shell 相关代码约 9,500 行，至少 23 个测试文件直接依赖这些路径。更重要的是，Cloud replacement 尚未存在；立即删除会同时失去可运行入口、持久 transcript、工具执行和大量回归证据，并迫使 Cloud 改造变成一次性重写。

成熟 Cloud Agent 也通常保留 local/dev runtime：LangGraph 提供无 Docker 的 `langgraph dev`，同时把同一 Agent Server 部署到 Cloud；OpenHands 用统一 workspace API 切换 local/docker/remote；GitHub Copilot 同时提供 local 与 cloud sandbox。这里保留的是工程适配器，不是双产品承诺。

参考：

- [LangGraph local development](https://docs.langchain.com/langsmith/local-dev-testing)
- [LangSmith deployment: same runtime, different hosting](https://docs.langchain.com/langsmith/deployment)
- [OpenHands Remote Agent Server](https://docs.openhands.dev/sdk/guides/agent-server/overview)
- [GitHub Copilot cloud and local sandboxes](https://docs.github.com/en/copilot/concepts/about-cloud-and-local-sandboxes)

### 删除顺序与触发条件

| 顺序 | 可删除/降级内容 | 前置条件 |
| --- | --- | --- |
| 1 | Local WebChannel、TUI/plain 作为正式入口 | Cloud Web API + 基本前端可完成 conversation/run 闭环 |
| 2 | setup wizard、本地 supervisor 的用户产品路径 | Cloud 配置、迁移、健康检查与进程部署入口完成 |
| 3 | SQLite SessionManager | PostgreSQL TranscriptStore 覆盖 append-only、搜索、删除、consolidation 合同 |
| 4 | asyncio MessageBus 作为核心 admission | durable RunStore + worker claim + per-conversation 串行通过故障测试 |
| 5 | host ShellProcessManager | SandboxExecutionBackend 覆盖 exec、PTY、stdin、cancel、timeout、cleanup |
| 6 | 不再需要的 QQ/本地渠道代码 | Cloud channel adapter 或明确产品下线决定完成 |

在删除完成前，目录与文档中应称其为 **迁移期 dev adapter**，不再称为 Local mode。最终可以保留
专用于单元测试的 fake/in-memory store、fake model 和 mock execution backend；它们不是可部署的
Local 产品，也不应带 TUI、workspace 配置或本机 shell 能力。

## 推荐下一纵向切片

在上述 P0 产品选择确认后，实现最小 Cloud slice：

```text
opaque auth session
  → create/list conversation
  → append user message + create queued run（同一事务）
  → 单 worker claim
  → 调用 TurnRequest/TurnResult
  → append assistant message + complete run（同一事务）
  → GET run/messages
```

这一切片先不用 Redis，也不执行 host shell。它先证明多用户隔离、会话不串、durable Run 和 Agent
core 复用；之后再拆 API/worker 并加入 Redis wakeup/SSE。

---

# 原始完整方案稿

> 以下是 2026-08-08 的原始方案全文，保留作为设计演进记录。其“Local/Cloud 双产品”定位已被本文件前半部分的 Cloud-only 决定取代；实施时以前半部分为准。

> 原始方案归档（2026-08-08）。本段只保留设计演进记录；确认状态与实施偏差以前文为准。

从“**真实产品需求 → 成熟系统怎么解决 → 再映射回 Kirakira 当前代码**”顺序完整收敛的方案初稿。

首先重点对照了目前几类成熟实现：LangGraph Agent Server 的 API/Queue Worker/Postgres/Redis/Checkpoint 架构、OpenHands 的 LocalWorkspace/RemoteWorkspace/Sandbox 抽象、Open WebUI 从 SQLite 单机到 PostgreSQL+Redis 多实例的扩容路径、Dify 的 Worker/Sandbox/SSRF Proxy 拆分，以及 PostgreSQL、Redis、MCP、Docker/gVisor、OWASP 等官方文档。下面不是“尽量把 Redis/MQ/微服务都塞进去”，而是每一个组件都对应具体问题。

# Kirakira Cloud 重构方案

## 0. 最终目标先定死

Kirakira 不应该被改造成另一个 Dify workflow builder，也不应该另起炉灶写成一个普通 FastAPI ChatBot。

最终产品是：

> **Kirakira：同时支持 Local-first 与 Cloud Multi-user 两种部署模式的通用 Agent Runtime。**

对用户而言，Cloud 版非常简单：

```text
User
├── Conversations
│   ├── Conversation A
│   ├── Conversation B
│   └── Conversation C
│
├── Global Memory
├── Files
├── Integrations / MCP
└── Scheduled Tasks
```

第一版**没有 Organization、Team、产品级 Workspace、RBAC**。

这里唯一存在的 workspace 是 Agent 执行时看不见的：

```text
Run
└── Sandbox
    └── /workspace
```

它只是计算环境，不是产品概念。

你的现有 Kirakira 则继续作为 Local 版：

```text
Kirakira Runtime
       │
 ┌─────┴───────┐
 │             │
Local        Cloud
 │             │
SQLite       PostgreSQL
Local FS     S3/MinIO
Local Shell  Sandbox
asyncio      Worker
Queue        Queue
```

这种“同一 Agent core，只更换 workspace/infrastructure”的路线不是我凭空设计的。OpenHands 当前 SDK 就明确把 `LocalWorkspace`、`DockerWorkspace`、`RemoteWorkspace` 放在统一 Workspace abstraction 后面，而且官方强调同一份 Agent/Conversation 代码可只通过更换 workspace 从本地运行切换到远程隔离执行。([[OpenHands Docs](https://docs.openhands.dev/sdk/api-reference/openhands.sdk.workspace)][1])

---

# 1. 你现在的 Kirakira 哪些要保留，哪些真正需要重构

先看现状。

Kirakira 已经不是一个简单 harness。现在 README 定义它为“本地优先、多渠道 AI Agent Runtime”，Passive Turn、Proactive、Drift 共用模型、工具、记忆和 Channel，而且已经有同 session 串行、跨 session 并行、Shell、子 Agent、Scheduler、MCP、插件、长期记忆和运行轨迹。

架构层面也已经有：

```text
agent/
bootstrap/
bus/
core/
infra/
session/
memory2/
plugins/
...
```

而且你原本就试图遵守“外层装配依赖内层合同”的 adapter 思路。

所以我们不要动 Agent 的灵魂部分。

| 当前模块                        | Cloud 后怎么处理                        |
| --------------------------- | ---------------------------------- |
| ReAct / Reasoner            | 保留                                 |
| ContextBuilder / compaction | 保留                                 |
| ToolRegistry                | 保留并增强                              |
| MemoryEngine protocol       | 保留                                 |
| MCP abstraction             | 保留                                 |
| Plugin hooks                | 保留，但 Cloud 权限收紧                    |
| Local Shell                 | Local 模式保留                         |
| SQLite SessionManager       | Local 模式保留                         |
| asyncio MessageBus          | Local 模式保留                         |
| TUI                         | Local 模式保留                         |
| Telegram                    | 作为外围 Channel adapter 保留            |
| QQ                          | 不删除，但停止重点开发                        |
| WebChannel                  | 降为 Local UI，Cloud 新建正式 FastAPI API |
| ShellProcessManager         | 抽成 ExecutionBackend                |
| SessionManager              | 抽成 Conversation/Transcript Store   |
| AgentLoop session lock      | Cloud 改成 durable run admission     |
| Scheduler                   | 拆“调度语义”和“本地持久化”                    |
| Proactive / Drift           | Local 保留，Cloud 后期接统一 Run 系统        |

最大的原则是：

> **保留业务能力，替换本地基础设施。**

---

# 2. 目标系统总体架构

最终 Cloud 版我建议收敛成：

```text
                         Browser
                           │
                    HTTP / SSE
                           │
                    ┌──────▼──────┐
                    │   FastAPI   │
                    │             │
                    │ Auth        │
                    │ Conversation│
                    │ Run API     │
                    │ File API    │
                    │ Memory API  │
                    └───┬─────┬───┘
                        │     │
              durable   │     │ coordination
                        │     │
               ┌────────▼─┐ ┌─▼────────┐
               │PostgreSQL│ │  Redis   │
               │          │ │          │
               │users     │ │wakeup    │
               │messages  │ │streaming │
               │runs      │ │cancel    │
               │memory    │ │rate limit│
               │checkpoint│ └────┬─────┘
               └─────┬────┘      │
                     │           │
                Run admission ◄──┘
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
       Worker     Worker      Worker
          │          │          │
          └──────────┼──────────┘
                     │
              Kirakira Runtime
                     │
          ┌──────────┼─────────────┐
          │          │             │
       LLM/API   Remote MCP    Tool Router
                                   │
                           ┌───────┴────────┐
                           │                │
                      safe tools      Sandbox tools
                           │                │
                      Web/API etc.   Sandbox Manager
                                            │
                                        gVisor /
                                      Docker Sandbox
                                            │
                                        /workspace

                   Object Storage
                  MinIO(dev) / S3
                         │
                   Files / Artifacts
```

这套架构和 LangGraph Agent Server 当前的设计非常接近：API server 创建 run，queue worker 执行；PostgreSQL 保存 thread/run/checkpoint/长期 memory，Redis 只承担 worker signalling、cancel、streaming 等 ephemeral coordination；API 和 Queue Worker 可以独立扩容。([[Docs by LangChain](https://docs.langchain.com/langsmith/agent-server)][2])

这正是我们需要的成熟参考。

---

# 3. 第一条最重要的架构原则：PostgreSQL 是 truth，Redis 不是数据库

这一点我会直接写成 ADR。

## ADR-001

> **所有用户和 Agent 的 durable state 以 PostgreSQL 为唯一事实源。Redis 只负责协调和加速。**

PostgreSQL 保存：

```text
User
Conversation
Message
Run
Run checkpoint
Tool call
Memory
File metadata
Schedule
Credential metadata
Usage
```

Redis 保存：

```text
worker wakeup
run cancel notification
live token/event pubsub
rate limiter
短 TTL cache
```

为什么？

Redis 官方明确说明 Pub/Sub 是 **at-most-once**：subscriber 断线时消息永久丢失；需要更强语义时才应该使用 Streams。([[Redis](https://redis.io/docs/latest/develop/pubsub/)][3])

LangGraph 当前也明确说 Redis 不保存任何 user/run data，它只是 server 与 queue worker 的通信层。([[Docs by LangChain](https://docs.langchain.com/langsmith/data-plane)][4])

所以：

```text
Redis 挂掉
```

最多应该导致：

```text
实时流慢了
worker 没马上被唤醒
cancel 延迟
rate limiter 降级
```

而不能导致：

```text
聊天丢了
run 丢了
memory 丢了
任务永远消失
```

---

# 4. PostgreSQL 为什么必须替换 Cloud 里的 SQLite

你当前 `SessionManager` 直接打开：

```python
sqlite3.connect(workspace / "sessions.db")
```

同时维护进程内 `_cache` 和 `threading.Lock`。

Local 版很好。

但 Cloud 多 worker 后：

```text
API 1
API 2
Worker 1
Worker 2
Worker 3
```

不能继续共享一个 SQLite 文件。

Open WebUI 的官方扩容指南正好给了几乎一模一样的发展路径：单用户默认 SQLite；一旦进入多个 worker/replica，就要求切换 PostgreSQL，并用 Redis 协调跨实例状态；1000+ 用户场景同样明确要求 PostgreSQL、Redis、外部向量 DB 等。([[Open WebUI](https://docs.openwebui.com/getting-started/advanced-topics/scaling/)][5])

所以本项目不是：

```text
SQLite → PostgreSQL
```

而是：

```text
LocalSessionStore → SQLite

CloudConversationStore → PostgreSQL
```

Local 能力不退化。

---

# 5. 数据模型：不要再用 channel:chat_id 作为身份

Cloud 版本的核心数据关系应该变成：

```text
User
 │
 ├── Conversation
 │      ├── Message
 │      ├── Run
 │      │    ├── Checkpoint
 │      │    └── ToolCall
 │      └── Artifact
 │
 ├── Memory
 ├── File
 ├── Credential
 ├── ChannelLink
 └── ScheduledTask
```

核心表我建议这样设计：

| 表                 | 关键字段                                                          |
| ----------------- | ------------------------------------------------------------- |
| `users`           | id, email, password_hash, created_at                          |
| `auth_sessions`   | token_hash, user_id, expires_at, revoked_at                   |
| `conversations`   | id, user_id, title, next_message_seq, timestamps              |
| `messages`        | id, conversation_id, seq, role, content, run_id               |
| `runs`            | id, user_id, conversation_id, input_message_id, status, lease |
| `run_checkpoints` | run_id, step_seq, state_json                                  |
| `tool_calls`      | id, run_id, step_seq, name, args, result, status              |
| `memories`        | id, user_id, summary, type, embedding, source                 |
| `memory_evidence` | memory_id, source_message_id                                  |
| `files`           | id, user_id, object_key, size, mime                           |
| `artifacts`       | id, run_id, file_id, kind                                     |
| `credentials`     | user_id, provider, encrypted_value, key_version               |
| `channel_links`   | user_id, provider, external_user_id                           |
| `scheduled_tasks` | user_id, schedule, prompt, next_run_at                        |
| `usage_ledger`    | user_id, run_id, provider, model, tokens                      |
| `outbox_events`   | 可选后期 MQ 用                                                     |

注意这里**没有 product workspace**。

---

# 6. Message 顺序怎么保证

不要：

```sql
SELECT MAX(seq) + 1
```

然后裸 INSERT。

并发时会冲突。

我建议给 `conversations` 增加：

```text
next_message_seq
```

新增消息时：

```sql
BEGIN;

SELECT id, next_message_seq
FROM conversations
WHERE id = :conversation_id
  AND user_id = :user_id
FOR UPDATE;

INSERT INTO messages (..., seq = next_message_seq);

UPDATE conversations
SET next_message_seq = next_message_seq + 1;

COMMIT;
```

数据库再加：

```sql
UNIQUE(conversation_id, seq)
```

应用层错误也不可能把顺序写乱。

---

# 7. Run 是整个 Cloud 架构里最重要的新概念

当前 Kirakira：

```text
InboundMessage
     ↓
AgentLoop
     ↓
pipeline.run()
     ↓
直接完成
```

Cloud 必须变成：

```text
Message
   ↓
Run
   ↓
queued
   ↓
running
   ↓
completed
```

状态机：

```text
queued
   │
   ▼
running ──────────────┐
 │    │               │
 │    ├──→ failed     │
 │    ├──→ cancelled  │
 │    └──→ waiting_user
 │
 ▼
completed
```

其中：

```text
Run ≠ HTTP request
```

这一条一定要彻底建立。

FastAPI 官方自己也明确提醒，重型、跨进程甚至跨服务器的后台计算不应该依赖 `BackgroundTasks`，而应该交给专门的任务队列/worker 系统。([[FastAPI](https://fastapi.tiangolo.com/tutorial/background-tasks/)][6])

因此 API：

```http
POST /v1/conversations/{id}/messages
```

只做：

```text
authenticate
↓
authorize
↓
insert user message
↓
insert run(status=queued)
↓
commit
↓
202 Accepted
```

返回：

```json
{
  "message_id": "...",
  "run_id": "..."
}
```

而不是等待 LLM。

---

# 8. 1000 个用户同时来时到底如何调度

比如：

```text
1000 个 run
```

Worker capacity 是：

```text
Worker 1: 10
Worker 2: 10
...
Worker 10: 10
```

则最多：

```text
100 running
900 queued
```

而不是创建 1000 个无上限 asyncio task。

LangGraph Agent Server 本身也是类似的有限 job-per-worker 模型，并明确建议根据 Agent 是 CPU-bound 还是 I/O-bound 调整单 worker concurrency，然后通过 queue-worker 数量水平扩容。([[Docs by LangChain](https://docs.langchain.com/langsmith/agent-server-scale)][7])

真正应该监控的指标是：

```text
queue_depth
queue_wait_seconds
active_runs
run_latency
```

而不是宣称：

> “系统支持 1000 个 Agent 同时运行。”

1000 用户在线和 1000 Agent 同时执行完全不同。

---

# 9. 同一 Conversation 串行，不同 Conversation 并行

这是你现在的：

```python
_session_locks[session_key] = asyncio.Lock()
```

真正要解决的问题。

Cloud 后不要继续依赖 Python lock。

数据库必须保证：

```text
Conversation A

run1 ────────→
              run2 ────────→
                             run3
```

而：

```text
Conversation A ───────────────→
Conversation B ───────────────→
Conversation C ───────────────→
```

可以并行。

Worker 的第一版 durable claim 可以使用 PostgreSQL：

```sql
FOR UPDATE SKIP LOCKED
```

PostgreSQL 官方明确指出，`SKIP LOCKED` 不适合一般查询的一致性语义，但非常适合多个 consumer 访问 queue-like table 时避免锁竞争。([[PostgreSQL](https://www.postgresql.org/docs/18/sql-select.html)][8])

再增加一道数据库不变量：

```sql
CREATE UNIQUE INDEX ...
ON runs(conversation_id)
WHERE status = 'running';
```

这样即便 claim 代码存在 race，也无法出现：

```text
同 conversation 两个 running
```

你当前 `session_admissions` 其实已经在 SQLite 里做了这个思想的雏形：通过 `session_key PRIMARY KEY` 阻止其他进程同时占有同一 session。

Cloud 只是把它正式提升为 durable run admission。

---

# 10. Redis 到底如何进入 Run Queue

这里我不建议一上来自己造 Redis Streams job system。

第一版采用：

```text
Postgres = durable queue state
Redis = worker wakeup
```

流程：

```text
API
 │
 ├── INSERT run(queued) → PostgreSQL
 │
 └── commit
        │
        └── Redis wakeup
```

Worker：

```text
wait Redis signal
       │
       ▼
query PostgreSQL
FOR UPDATE SKIP LOCKED
       │
       ▼
claim run
```

同时 worker 每隔几秒进行一次 DB fallback scan。

所以即使发生：

```text
DB commit
   ↓
API crash
   ↓
没来得及 Redis wakeup
```

run 也不会丢，只会被 fallback polling 稍晚发现。

这和 LangGraph 当前“Redis wake worker，但 run detail 从 PostgreSQL 加载”的思路是一致的，而且其 2026 年 changelog 也显示他们在高负载下将 queue hot path 从 PostgreSQL polling 向 Redis 优化，但 run execution order 和 durable state 仍然受持久层约束。([[Docs by LangChain](https://docs.langchain.com/langsmith/agent-server-changelog)][9])

这是很漂亮的一条演进路径：

```text
正确性先在 PG
        ↓
Redis 只是 latency optimization
        ↓
真正出现瓶颈再优化 queue hot path
```

---

# 11. 那还需要 RabbitMQ 吗？

**V1 不需要。**

因为现在引入：

```text
Postgres
Redis
RabbitMQ
```

却没有第三种独立需求，是为了简历堆技术。

但是在后期可以做一个非常合理的扩展：

```text
Postgres
   │
Transactional Outbox
   │
Outbox Relay
   │
RabbitMQ
   │
Workers
```

为什么不能直接：

```python
db.commit()
rabbitmq.publish()
```

因为存在 dual-write：

```text
DB 成功，MQ 失败
```

或者反过来。

Transactional Outbox 的标准方案是业务数据和 outbox row 放在同一个 DB transaction，之后由 relay 异步发布。([[microservices.io](https://microservices.io/patterns/data/transactional-outbox.html?source=post_page-----fa3de00ceba5---------------------------------------)][10])

如果真做到 RabbitMQ 版本，我会用 quorum queue + publisher confirms + manual consumer ack；RabbitMQ 官方推荐 quorum queue 用于需要 durable/high-availability 的关键队列，并强调 confirms/acks 是数据安全的一部分。([[RabbitMQ 博客](https://blog.rabbitmq.com/docs/4.1/quorum-queues)][11])

所以：

> RabbitMQ 是优秀的 Phase 2/advanced backend extension，但不是 Cloud MVP 的必要组成。

---

# 12. Worker crash 怎么办：lease + heartbeat

每条 running run 保存：

```text
lease_owner
lease_expires_at
heartbeat_at
attempt
```

例如：

```text
worker-7
lease expires = now + 60s
```

worker 每隔一段时间续 lease。

如果：

```text
worker-7 crash
```

则：

```text
lease expires
     ↓
reaper detects
     ↓
run → queued
```

重新调度。

但这里马上出现一个 Agent 特有问题：

> “重新执行整个 Agent turn，会不会重复调用已经执行过的工具？”

所以仅有 queue retry 还不够。

---

# 13. Cloud Agent 必须增加 durable checkpoint

这是整个项目从“Web 后端”升级成真正 **Agent Infrastructure** 的关键点。

LangGraph Agent Server 会在 graph execution step 后保存 checkpoint，因此 worker 中断后可以从上一个 checkpoint 恢复，而不是整轮从头执行。([[Docs by LangChain](https://docs.langchain.com/langsmith/agent-server)][2])

Kirakira 应该借鉴这个机制。

每个 ReAct iteration 完成一个安全边界后：

```text
LLM
 ↓
tool call(s)
 ↓
tool result(s)
 ↓
checkpoint
```

持久化：

```text
run_checkpoints

run_id
step_seq
state_json
created_at
```

`state_json` 至少包含：

```text
当前 model transcript
iteration
tool chain
usage
compaction state
unlocked tools
sandbox_id
```

worker 恢复：

```text
load latest checkpoint
        ↓
continue iteration N+1
```

而不是：

```text
重新读 conversation
重新把整个 request 从头跑
```

---

# 14. 外部副作用 Tool 不能傻瓜式 retry

你的 `ToolRegistry` 现在已经非常幸运地具有：

```text
risk =
  read-only
  write
  external-side-effect
```

以及 builtin/MCP/plugin 来源信息。

这非常适合继续扩展。

我建议 `ToolMeta` 演化成：

```python
@dataclass(frozen=True)
class ToolMeta:
    risk: Literal[
        "read-only",
        "write",
        "external-side-effect",
    ]

    execution_target: Literal[
        "worker",
        "sandbox",
        "remote_mcp",
        "delivery",
    ]

    network_policy: Literal[
        "none",
        "internet",
        "allowlist",
    ]

    retry_policy: Literal[
        "safe",
        "idempotent",
        "never",
    ]

    requires_confirmation: bool = False
```

因为：

```text
web_search
```

worker crash 后再调用一次一般没问题。

但：

```text
send_email
delete_github_issue
transfer_money
post_message
```

不能默认重复执行。

`tool_calls` 应该有：

```text
planned
executing
succeeded
failed
unknown
```

对于 provider 支持 idempotency key 的外部操作，使用：

```text
run_id + tool_call_id
```

作为幂等键。

如果 worker 在：

```text
请求实际成功
     ↓
来不及写 succeeded
     ↓
crash
```

那么恢复后是：

```text
unknown
```

不能自动再发一次。

这一层非常能体现真正 Agent backend 的工程深度。

---

# 15. Event Streaming：选择 SSE，而不是 WebSocket

Cloud Web UI 的交互实际上是：

```text
Browser → Server
POST message
POST cancel
POST approval
```

而实时数据主要：

```text
Server → Browser
token
thinking
tool event
status
```

因此主协议选：

> **HTTP + SSE**

而不是整个系统都 WebSocket。

SSE 原生就是 server→client 单向流，浏览器支持 reconnect，也支持 event ID。([[MDN Web Docs](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)][12])

LangGraph Agent Server 同样把 run stream 通过 Server-Sent Events 转发给客户端。([[Docs by LangChain](https://docs.langchain.com/langsmith/agent-server)][2])

API：

```http
GET /v1/runs/{run_id}/stream
Accept: text/event-stream
```

可能输出：

```text
event: run.started

event: model.delta
data: {...}

event: tool.started
data: {...}

event: tool.completed
data: {...}

event: run.completed
data: {...}
```

---

# 16. 但是不要每个 token 都 INSERT PostgreSQL

这是另一处很容易过度设计的地方。

不要：

```text
token 1 → DB
token 2 → DB
token 3 → DB
...
```

会制造惊人的写放大。

建议分两层：

```text
Redis Pub/Sub
    ↓
live token delta
```

和：

```text
PostgreSQL
    ↓
durable semantic events
checkpoint
final message
```

例如 PostgreSQL 记录：

```text
run.started
model.completed
tool.started
tool.completed
checkpoint.saved
run.completed
```

token delta 只走 Redis。

Redis Pub/Sub 丢了一个 transient delta 并不可怕，因为最终回复和 checkpoint 都存在 PostgreSQL；而且 Redis 官方明确说 Pub/Sub 本身就是 at-most-once。([[Redis](https://redis.io/docs/latest/develop/pubsub/)][3])

SSE 重连时：

```text
GET run current state
        ↓
拿 latest checkpoint/output
        ↓
重新订阅 live events
```

而不是依赖 Redis 给你重放全部 token。

---

# 17. Cancellation 也必须有 durable + fast 两条路

API：

```http
POST /v1/runs/{id}/cancel
```

先：

```sql
UPDATE runs
SET cancel_requested_at = now();
```

这是 durable truth。

同时：

```text
Redis Pub/Sub
    ↓
worker 收到立即 cancel
```

worker 则在：

```text
LLM call 前
tool call 前
ReAct iteration 边界
Sandbox exec 等待过程中
```

检查 cancel。

所以 Redis cancel 丢了，也只是取消稍慢。

---

# 18. Shell 不删，而是正式抽象成 ExecutionBackend

你当前 Shell 已经是纯本地实现：`shell_command.py` 会寻找宿主系统上的 bash/zsh/powershell，而 `ShellProcessManager` 内部直接持有 `asyncio.subprocess.Process`、execution ID 和本机 log file。

这部分正是最应该抽象的地方。

定义：

```python
class ExecutionBackend(Protocol):
    async def start(self, context) -> ExecutionWorkspace: ...
    async def exec(self, workspace, request) -> ExecResult: ...
    async def read_file(self, workspace, path) -> bytes: ...
    async def write_file(self, workspace, path, data) -> None: ...
    async def kill(self, workspace, execution_id) -> None: ...
    async def close(self, workspace) -> None: ...
```

Local：

```text
LocalExecutionBackend
     ↓
现有 ShellProcessManager
```

Cloud：

```text
SandboxExecutionBackend
     ↓
Sandbox Manager API
     ↓
isolated container
```

这正是 OpenHands 当前 Workspace 架构的核心责任：command execution、file operations、resource lifecycle、environment isolation 都落在统一 Workspace seam 后面。([[OpenHands Docs](https://docs.openhands.dev/sdk/arch/workspace)][13])

---

# 19. Sandbox 架构

我不建议让：

```text
Agent Worker
     ↓
/var/run/docker.sock
```

直接控制宿主 Docker。

更干净的是：

```text
Worker
   │
   │ narrow internal API
   ▼
Sandbox Manager
   │
   ▼
Container Runtime
   │
   ▼
Sandbox
```

Sandbox Manager 才能接触 container runtime。

API / Worker 永远拿不到 Docker socket。

一轮：

```text
run starts
   ↓
create sandbox
   ↓
hydrate files
   ↓
Agent executes
   ↓
upload artifacts
   ↓
destroy sandbox
```

---

# 20. Sandbox 技术选型：Docker 和 gVisor怎么选

这里我不会说“Docker 就安全了”。

### 开发版本

用：

```text
Rootless Docker
+ non-root container user
+ cgroup resource limits
+ pids limit
+ seccomp
+ drop capabilities
+ network restricted
```

Docker 官方 rootless 模式使 daemon 和 container 都运行在非 root user namespace 中，用于降低 daemon/runtime 漏洞带来的风险。([[Docker Documentation](https://docs.docker.com/engine/security/rootless/)][14])

Docker 默认 seccomp profile 也会禁止数十种敏感 syscall，但官方只把它称为 moderately protective。([[Docker Documentation](https://docs.docker.com/engine/security/seccomp/)][15])

因此，对于真正“陌生用户可以让 Agent 执行任意 Bash”的 hostile workload：

### Production-like target

我建议：

```text
Docker API
   ↓
gVisor runsc
```

gVisor 本身就是为隔离 untrusted userspace code、减少直接暴露宿主 Linux kernel attack surface 而设计。([[gVisor](https://gvisor.dev/docs/architecture_guide/security/)][16])

也就是说简历里可以明确说：

> 本地开发使用 Rootless Docker；production-like sandbox 使用 gVisor runtime。

这比简单一句“Docker Sandbox”严谨得多。

---

# 21. Sandbox 的网络默认不应该完全开放

如果允许用户输入：

```text
curl http://169.254.169.254
```

或者：

```text
curl http://postgres:5432
curl http://redis:6379
```

那就会产生 SSRF / internal network probing 问题。

因此：

```text
Sandbox
   │
   └── controlled egress
           ↓
       Egress Proxy
```

默认禁止访问：

```text
localhost
RFC1918 private networks
cloud metadata endpoints
internal service network
```

Dify 当前 Compose 就把 Sandbox 接到单独 SSRF Proxy，并通过 HTTP_PROXY/HTTPS_PROXY 控制 sandbox 网络访问。([[GitHub](https://github.com/langgenius/dify/blob/main/docker/docker-compose-template.yaml)][17])

这是值得直接借鉴的成熟设计。

---

# 22. Tool 应该按执行环境分类，而不是按名字分类

最终：

| Tool            | execution target     |
| --------------- | -------------------- |
| web_search      | Worker               |
| HTTP API        | Worker + SSRF policy |
| GitHub API      | Worker               |
| read_file       | Sandbox              |
| write_file      | Sandbox              |
| bash            | Sandbox              |
| python          | Sandbox              |
| git             | Sandbox              |
| stdio MCP       | Sandbox              |
| remote HTTP MCP | Worker               |
| Telegram send   | Delivery service     |

于是 Agent 仍然只是：

```text
LLM
 ↓
tool_call("bash")
```

完全不知道：

```text
Local
还是
Cloud Sandbox
```

这是非常重要的边界。

---

# 23. MCP 怎么处理

MCP 官方目前定义的标准 transport 就是：

```text
stdio
Streamable HTTP
```

stdio MCP 由 client 启动一个子进程；Streamable HTTP MCP 则是独立 HTTP service。([[Model Context Protocol](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports)][18])

所以天然对应：

```text
stdio MCP
    ↓
Sandbox
```

因为它本质是：

```text
spawn subprocess
```

而：

```text
Streamable HTTP MCP
    ↓
Worker → HTTP
```

但 user supplied URL 必须经过：

```text
URL validation
DNS resolution protection
SSRF egress policy
auth
```

不能允许：

```text
http://postgres/
http://localhost/
http://169.254...
```

---

# 24. Plugin 在 Cloud 模式必须收紧

你现在 Local 版插件可以：

```text
动态加载 Python
热重载
注册工具
注册 channel
```

这是个人电脑上很好的能力。

Cloud 如果允许：

> “User A 上传任意 Python plugin，然后 API worker import”

等于直接给 SaaS 用户 RCE。

所以第一版 Cloud：

```text
Server bundled trusted plugins
            ✓

Admin-installed trusted plugins
            ✓

User-uploaded arbitrary Python plugin
            ✗
```

用户自己的可执行扩展：

```text
stdio MCP
```

放 Sandbox。

Remote integration：

```text
Streamable HTTP MCP
```

走网络。

Dify 当前也把 sandbox、plugin/agent runtime、worker 分离，而不是简单把所有用户扩展 import 到 API process。([[GitHub](https://github.com/langgenius/dify/blob/main/docker/docker-compose-template.yaml)][17])

---

# 25. 文件系统：Postgres 不存文件本体

用户上传：

```text
paper.pdf
repo.zip
dataset.csv
```

PostgreSQL 只存：

```text
file_id
user_id
object_key
mime
size
checksum
```

文件本身：

```text
MinIO   ← local/docker-compose
S3      ← production
```

推荐上传流程：

```text
Browser
   │
   │ request upload
   ▼
FastAPI
   │
   │ create file metadata
   │ generate presigned URL
   ▼
Browser ───────────→ Object Storage
```

而不是：

```text
Browser
  ↓ 4GB file
FastAPI
  ↓
Python
  ↓
S3
```

S3 官方 presigned URL 就是专门用于在不向 client 发 AWS credentials 的情况下，授权一定时间内上传指定对象。([[AWS 文档](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)][19])

---

# 26. Sandbox 怎么拿到用户文件

Run 启动：

```text
PostgreSQL files metadata
       │
       ▼
Object Storage
       │
       ▼
Sandbox /workspace
```

Agent 处理：

```text
/workspace/data.csv
/workspace/project/
```

产生：

```text
result.csv
patched_repo.zip
chart.png
```

run 完成前：

```text
Sandbox
   ↓ upload
Object Storage
   ↓
artifacts row
```

然后 Sandbox 可以销毁。

所以重要数据永远不以 sandbox 为权威位置。

---

# 27. Sandbox 生命周期第一版不要做得太复杂

V1：

> **Run-level ephemeral sandbox**

```text
run starts
   ↓
sandbox starts
   ↓
run ends
   ↓
artifact persist
   ↓
sandbox destroy
```

一个 run 里面后台 Shell/PTY 可以继续活着。

不同 run 之间不保留 installed dependencies。

以后如果 coding use case 确实证明每轮重装依赖很浪费，再升级：

```text
Conversation
      ↓
Warm Sandbox
      ↓
idle TTL
      ↓
snapshot/destroy
```

不要第一版就做 persistent container pool。

---

# 28. Memory 不要推倒重写

你现在的 Memory 层反而是项目设计最适合保留的部分之一。

现在已经有统一 `MemoryEngine` protocol，把 ingest/query/mutate/admin 与具体 Default/Akasha 实现隔离。

而记忆架构也明确把：

```text
会话 truth
context projection
structured memory
Markdown memory
```

分开。

Cloud 要做的不是重新研究记忆算法。

而是把：

```python
MemoryScope(
    session_key,
    channel,
    chat_id
)
```

演化成：

```python
MemoryScope(
    user_id,
    conversation_id=None,
    channel=None,
    external_chat_id=None,
)
```

其中默认长期记忆：

```text
scope = user
```

所以：

```text
Conversation A ─┐
Conversation B ─┼──→ User Global Memory
Conversation C ─┘
```

符合我们之前确定的产品形态。

---

# 29. Cloud Memory 存 PostgreSQL + pgvector

第一版：

```text
memories
---------
id
user_id
kind
summary
embedding
source_message_id
created_at
metadata
```

embedding 用 `pgvector`。

但也不要一上来：

```text
必须 HNSW
```

pgvector 默认精确 nearest-neighbor search，perfect recall；HNSW/IVFFlat 是为了数据规模增长后换取性能。官方说明 HNSW query speed-recall 通常比 IVFFlat 更好，但 build 更慢、内存占用更大。([[GitHub](https://github.com/pgvector/pgvector)][20])

所以正确演进：

```text
初期 exact search
      ↓
真实 benchmark
      ↓
数据量/latency 上升
      ↓
HNSW
```

你现有：

```text
RRF
keyword lane
structured memory
evidence
```

继续保留。

只换 store。

---

# 30. Auth 不采用“JWT 扔 localStorage”

你现在 WebChannel 是：

```javascript
localStorage.kirakiraSessionId ||
crypto.randomUUID()
```

这只是本地身份标签，不是真正认证。

Cloud 浏览器端我建议：

> **opaque server-side session + HttpOnly cookie**

登录：

```text
email/password
      ↓
Argon2id verify
      ↓
random 256-bit session token
      ↓
hash(token) → auth_sessions DB
      ↓
raw token → HttpOnly Cookie
```

Cookie：

```text
__Host-kirakira_session
Secure
HttpOnly
SameSite=Lax/Strict
Path=/
```

OWASP 明确推荐 cookie 作为 web session ID 交换机制，并明确警告不要把 JWT、session ID、refresh token 等认证凭证放进 localStorage；同时推荐 `Secure`、`HttpOnly`、`SameSite` 属性。([[OWASP Cheat Sheet Series](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)][21])

密码使用 Argon2id，OWASP 当前仍将其作为首选。([[OWASP Cheat Sheet Series](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)][22])

API Client 后面再提供：

```text
Personal Access Token
```

两套 auth 不混。

---

# 31. 多用户隔离至少做两层

第一层，Application Authorization：

```python
conversation = await repo.get(
    conversation_id=id,
    user_id=current_user.id,
)
```

永远不：

```python
repo.get(id)
```

第二层，PostgreSQL Row-Level Security 作为 defense-in-depth。

PostgreSQL 原生支持 RLS；开启后可以限制不同数据库用户/上下文能看到或修改哪些 row，无 policy 时为 default deny。([[PostgreSQL](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)][23])

例如 Cloud API runtime role：

```sql
ALTER TABLE conversations
ENABLE ROW LEVEL SECURITY;

CREATE POLICY own_conversations
ON conversations
USING (
  user_id =
  current_setting('app.user_id')::uuid
);
```

每个事务：

```sql
SET LOCAL app.user_id = '...';
```

这里必须注意 API runtime DB role 不能是拥有 `BYPASSRLS` 的超级角色。

Worker 因为需要 claim 全局 run，可以使用另外一个 service DB role。

这样能体现：

```text
Application authorization
        +
Database isolation
```

两层防御。

---

# 32. SQLAlchemy 技术栈

我建议：

```text
SQLAlchemy 2.x
Psycopg 3
Alembic
```

原因不是“大家都这么写”。

SQLAlchemy 当前 PostgreSQL dialect 官方同时支持 asyncpg 与 psycopg 3，而 psycopg 3 dialect 本身可以由同一个 dialect 同时提供 sync/async implementation。([[SQLAlchemy 文档](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html)][24])

因此：

```text
FastAPI / Worker
    ↓
create_async_engine()
    ↓
psycopg3 async
```

migration 使用 Alembic。

并遵循：

```text
一个 HTTP request / 一个 worker transaction
        ↓
自己的 AsyncSession
```

SQLAlchemy 官方明确说明 `AsyncSession` 是 mutable/stateful transaction object，不能在多个 concurrent asyncio tasks 之间共享。([[SQLAlchemy 文档](https://docs.sqlalchemy.org/en/21/orm/extensions/asyncio.html)][25])

---

# 33. Credential / API Key 怎么存

第一版模型调用最好：

```text
Server-managed provider credentials
```

即产品自己的 OpenAI/DeepSeek/etc key。

后面可以提供 BYOK。

如果用户配置：

```text
GitHub token
Remote MCP token
模型 API key
```

不能明文：

```text
credentials.api_key
```

建议：

```text
credentials
---------
ciphertext
nonce
key_version
provider
user_id
```

生产环境 encryption root key：

```text
KMS / Vault / Secret Manager
```

而不是：

```text
数据库旁边放 MASTER_KEY.txt
```

OWASP Secrets Management 也明确推荐集中化 secret lifecycle，并强调 encryption key 不应与加密的 secret 放在一起，生产可借助 KMS/Secret Manager/Vault 等 envelope encryption。([[OWASP Cheat Sheet Series](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)][26])

本地开发环境可以使用独立 dev key。

---

# 34. Rate Limit

Redis 非常适合这一层。

比如：

```text
per user:
20 run submissions / min

per IP:
10 login attempts / min

per user:
4 concurrent running agents

global:
100 concurrent sandbox
```

Redis 官方本身就列出了 fixed-window、sliding-window、token-bucket 等 rate limiting 模型，并适合多个 service instance 共享计数。([[Redis](https://redis.io/docs/latest/develop/use-cases/rate-limiter/)][27])

注意这里有两种 limit：

```text
HTTP request rate limit
```

和：

```text
Agent resource quota
```

是不同问题。

---

# 35. 多渠道不删除，但退出核心 domain

现在：

```text
channel + chat_id
     ↓
session key
```

Cloud 改为：

```text
Channel adapter
     ↓
Identity Resolver
     ↓
user_id
conversation_id
     ↓
RunService
```

数据库：

```text
channel_links

user_id
provider
external_user_id
external_chat_id
```

例如 Telegram：

```text
telegram user 123
       ↓
channel_links
       ↓
Kirakira user A
       ↓
conversation B
```

因此 Agent Runtime 不再关心：

> “你到底是 Telegram 用户还是 Web 用户。”

---

# 36. Web 不再视为普通 Channel

Cloud 中：

```text
Web
```

就是主产品 API。

所以现有：

```text
infra/channels/web_chat_channel.py
```

保留给：

```text
local mode
```

它现在使用 `ThreadingHTTPServer`、进程内 `_pending Future` 和 `_event_queues`，并在 POST 请求中同步等待 Agent reply。

Cloud 新增：

```text
services/api/
```

完全不经过这个 WebChannel。

---

# 37. Scheduler 也需要服务化

你当前 `SchedulerService`：

```text
local JSON file
_jobs dict
asyncio.Event
APScheduler CronTrigger
```

其中：

```text
时间解析 / cron 计算
```

很好，保留。

需要替换的是：

```text
local persistence
local wake loop
```

Cloud：

```text
scheduled_tasks
---------------
user_id
conversation_id
trigger_type
cron_expr
timezone
next_run_at
status
prompt
```

专门：

```text
scheduler service
```

使用：

```sql
FOR UPDATE SKIP LOCKED
```

claim 到期 schedule。

然后**不要自己执行 Agent**。

只是：

```text
schedule fires
     ↓
create normal Run
     ↓
Worker
```

因此：

```text
普通聊天
定时任务
未来 proactive
未来 drift
```

最终全部汇聚到同一个 Run infrastructure。

---

# 38. 为什么现在不直接用 APScheduler 4 distributed scheduler

因为当前 APScheduler 4 文档虽然已经重构为共享 datastore + worker 模式，但官方版本历史仍明确警告 4.0 series 是 pre-release，可能发生无迁移路径的 backward-incompatible changes，并明确写着不要用于 production。([[APScheduler](https://apscheduler.readthedocs.io/en/master/versionhistory.html)][28])

你的项目现在 pin 的也是：

```text
APScheduler >=3.11,<4
```

所以 Cloud 第一版直接设计简单的：

```text
scheduled_tasks + scheduler worker
```

更稳。

现有 APScheduler 3 `CronTrigger` 可以继续当 cron next-fire calculator。

---

# 39. Proactive / Drift 第一阶段不 Cloud 化

这是我明确建议“暂时不做”的东西。

原因不是没价值。

而是 Cloud 后：

```text
1000 users
×
Proactive tick
×
Drift background agent
```

会产生非常大的：

```text
cost
quota
scheduler
fairness
background sandbox
notification
```

复杂度。

所以：

### Local

```text
Proactive ✓
Drift ✓
```

### Cloud V1

```text
Passive Agent ✓
Scheduled Tasks ✓

Proactive 暂缓
Drift 暂缓
```

以后让它们统一创建：

```text
Run.origin =
  user
  schedule
  proactive
  drift
```

而不是再造独立执行系统。

这里的“暂缓”只表示 Cloud 调度、租户状态与成本治理后置，**不表示允许实现简化版算法**。
进入 Cloud 阶段时必须保留并复用现有实现中的：

- Proactive 多时间尺度 energy 衰减、soft-or base score、阈值与 jitter；
- Drift 空闲驱动积分、随机 hazard 目标、近期 Drift 抑制、重复度抑制接口和到期时间求解；
- `min_interval_hours` 硬门、timer anchor 变化重采样、到期只触发一次和跨重启排程；
- 最久未运行 skill 优先、强制工具轮次与 `finish_drift` 收尾；
- journal、continuum、真实投递成功后才写会话的提交语义。

Cloud 改造只把当前 SQLite/global state 换成按 `user_id`（必要时再按 conversation）隔离的
PostgreSQL adapter，把本地循环换成可抢占的 durable schedule/Run，并把 MessageBus 投递换成 Cloud
delivery port。算法函数应直接调用原模块，不能复制一份“近似逻辑”。原有数学、采样、重启连续性和
端到端测试必须成为 Cloud adapter 的合同测试；若未来确需调整算法，只能作为独立产品/算法变更评审，
不能夹带在后端重构中。

当前实现审计需特别保留一个已知差异：实际 `_hazard_due()` 使用
`sample_drift_delay_hours()`，并暂时把 `repetition_suppression` 传为 `0.0`；
`advance_drift_drive()` 的 content/repetition/threshold 推进公式当前没有接入真实 runner。Cloud Drift
开工前必须对照原始目标或 upstream 决定正确接线，并为选定行为补 golden tests，不能把当前未接线状态
悄悄固化成“简化版 parity”。

---

# 40. Runtime 最关键的一次代码重构：Channel Message 与 Agent Turn 解耦

现在 `DefaultReasoner.run_turn()` 和 Pipeline 仍明显接收 `InboundMessage`、channel、chat_id。

Cloud 后应该出现一个 canonical domain object：

```python
@dataclass(frozen=True)
class TurnRequest:
    run_id: str
    user_id: str | None
    conversation_id: str

    content: str
    attachments: tuple[AttachmentRef, ...]

    origin: Literal[
        "user",
        "schedule",
        "proactive",
        "drift",
    ]

    channel: str | None = None
    external_chat_id: str | None = None
```

Reasoner 真正需要的是：

```text
content
history
memory
tool context
execution context
```

而不是：

```text
Telegram chat_id
```

Channel 只是 metadata。

---

# 41. 再把 Pipeline 从“发送回复”改成“返回结果”

目前大致：

```text
MessageBus
  ↓
AgentLoop
  ↓
PassiveTurnPipeline
  ↓
bus.publish_outbound()
```

Cloud core 应该更接近：

```python
result = await turn_executor.execute(
    request,
    event_sink=...,
)
```

得到：

```python
TurnResult(
    reply=...,
    tool_chain=...,
    usage=...,
    checkpoint=...,
)
```

然后：

### Local Adapter

```text
TurnResult
   ↓
MessageBus
   ↓
Telegram / TUI / WebChannel
```

### Cloud Worker

```text
TurnResult
   ↓
Postgres
   ↓
Redis event
```

这是整个重构最关键的 inversion。

---

# 42. 现有 ports.py 是最合适的起点

你现在 `agent/looping/ports.py` 已经写得很接近这个方向：

> 配置参数和 service object 分离，让 pipeline 只依赖服务包，替换 session/context implementation 不需要改 pipeline。

所以不用重新发明架构。

把现有 seam 真正扩充为：

```python
@dataclass
class ConversationServices:
    transcript: TranscriptStore

@dataclass
class RunServices:
    checkpoints: CheckpointStore
    events: RunEventSink

@dataclass
class ExecutionServices:
    workspace: ExecutionBackend

@dataclass
class FileServices:
    objects: ObjectStore

@dataclass
class IdentityServices:
    principals: PrincipalResolver
```

即可。

---

# 43. 我建议的目录重构

不要第一天搬整个仓库。

最终逐渐变成：

```text
agent/
    core/
    model_runtime/
    prompting/
    tools/
    mcp/
    plugins/
    retrieval/
    ...
        # Agent reasoning / capability

core/
    domain/
        identity.py
        conversation.py
        run.py
        events.py
        execution.py

    ports/
        transcript.py
        run_store.py
        checkpoint.py
        event_sink.py
        execution.py
        object_store.py
        secret_store.py

    memory/
        ...
        # existing memory contracts

infra/
    local/
        sqlite_transcript.py
        local_execution.py
        local_object_store.py
        local_run_adapter.py

    postgres/
        models/
        repositories/
        memory_store.py

    redis/
        wakeup.py
        pubsub.py
        rate_limit.py

    object_storage/
        s3.py

    sandbox/
        client.py
        manager/
        docker_runtime.py
        gvisor_runtime.py

    channels/
        telegram/
        qq/
        ...

services/
    api/
        main.py
        auth/
        routes/
        dependencies.py

    worker/
        main.py
        runner.py
        admission.py
        recovery.py

    scheduler/
        main.py

bootstrap/
    local.py
    cloud_api.py
    cloud_worker.py
    cloud_scheduler.py
```

注意：

> 不要创建 `cloud_agent/` 再复制一份 Reasoner。

Local / Cloud 必须共享同一个 Agent core。

---

# 44. `bootstrap/app.py` 必须拆

现在 `bootstrap/app.py` 一次性装配：

```text
MessageBus
SessionManager
Memory
ToolRegistry
MCP watcher
Reasoner
Subagent
Scheduler
PluginManager
Channels
Proactive
Drift
Control plane
```

这在 single-process Local 应用完全合理。

Cloud 以后必须至少有：

```text
bootstrap/local.py

bootstrap/cloud_api.py

bootstrap/cloud_worker.py

bootstrap/cloud_scheduler.py
```

因为 API container 根本不应该初始化：

```text
ShellProcessManager
MCP stdio processes
Proactive loop
Agent Reasoner
```

API 只处理 HTTP/domain service。

Worker 才装 Agent Runtime。

---

# 45. Cloud API 的具体 endpoint

我建议第一版定成：

```text
POST   /v1/auth/register
POST   /v1/auth/login
POST   /v1/auth/logout
GET    /v1/me

GET    /v1/conversations
POST   /v1/conversations
GET    /v1/conversations/{id}
DELETE /v1/conversations/{id}

GET    /v1/conversations/{id}/messages
POST   /v1/conversations/{id}/messages

GET    /v1/runs/{id}
GET    /v1/runs/{id}/stream
POST   /v1/runs/{id}/cancel

GET    /v1/memories
DELETE /v1/memories/{id}

POST   /v1/files/upload-url
POST   /v1/files/{id}/complete
GET    /v1/files
GET    /v1/files/{id}/download-url

GET    /v1/integrations
POST   /v1/integrations/mcp
DELETE /v1/integrations/{id}

GET    /v1/schedules
POST   /v1/schedules
DELETE /v1/schedules/{id}
```

没有：

```text
POST /chat → 等 2 分钟
```

---

# 46. 完整的一次用户请求最后应该长这样

用户：

> “运行我刚才上传的项目，看看 pytest 为什么失败。”

```text
Browser
   │
   ▼
FastAPI
   │
   ├─ auth cookie → user_id
   ├─ authorize conversation
   ├─ insert Message
   ├─ insert Run(queued)
   └─ COMMIT
          │
          ├──── Redis wakeup
          │
          ▼
       202 response

Worker
   │
   ├─ claim Run
   ├─ lease
   ├─ load conversation
   ├─ retrieve user memory
   └─ start sandbox
          │
          ▼
     Kirakira Runtime
          │
        ReAct
          │
     tool: read_file
          │
          ▼
       Sandbox
          │
     tool: bash(pytest)
          │
          ▼
       Sandbox
          │
       error
          │
          ▼
         LLM
          │
     tool: write_file
          │
          ▼
       Sandbox
          │
     tool: bash(pytest)
          │
          ▼
        success

每个 safe step
    ↓
Postgres checkpoint

实时 delta
    ↓
Redis PubSub
    ↓
FastAPI SSE
    ↓
Browser

最终
    ↓
assistant Message
Run completed
usage ledger
artifact → S3
sandbox destroy
```

这已经是一个非常完整的 Agent production architecture。

---

# 47. 部署方案

第一阶段**不要 Kubernetes**。

Docker Compose 足够：

```text
kirakira-api
kirakira-worker
kirakira-scheduler

postgres
redis
minio

sandbox-manager
ssrf-proxy

otel-collector        optional initially
prometheus            optional initially
grafana               optional initially
```

然后：

```text
docker compose up --scale worker=4
```

就能真实测试 horizontal worker scaling。

FastAPI 官方在由容器编排层做 replication 时也推荐“一 container 一 Uvicorn process”，而不是每个 container 再套复杂 worker manager。([[FastAPI](https://fastapi.tiangolo.com/deployment/docker/)][29])

所以：

```text
api replica 1 = one Uvicorn
api replica 2 = one Uvicorn
```

非常干净。

---

# 48. Observability

这里不建议自己写 dashboard-only telemetry。

采用：

```text
OpenTelemetry
    ↓
OTel Collector
    ↓
Prometheus / tracing backend
    ↓
Grafana
```

Python OpenTelemetry 已有 FastAPI 官方 instrumentation，可自动创建 HTTP spans，也可以给 Agent 内部手动增加 child spans。([[OpenTelemetry Python Contrib](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html)][30])

我建议 trace：

```text
HTTP request
 └─ create_run
     └─ worker_claim
         └─ agent_run
             ├─ llm_call
             ├─ memory_retrieve
             ├─ tool_call
             │   └─ sandbox_exec
             └─ checkpoint
```

关键 metrics：

```text
run_queue_depth
run_queue_wait_seconds
active_runs
run_duration_seconds

llm_latency
llm_error_rate
prompt_tokens
completion_tokens

tool_latency
tool_error_rate

sandbox_start_seconds
sandbox_active
sandbox_failures

redis_errors
postgres_pool_usage

sse_connections
```

---

# 49. Load Test 不要调用真的 LLM

你要测试的是：

```text
后端并发
```

而不是：

```text
OpenAI API 能扛多少
```

所以构造：

```text
MockModelClient
latency = random 2~8 sec
stream chunks
偶尔 tool call
```

然后压：

```text
1000 users
1000 SSE connections
10k conversations
burst 1000 run submissions
```

检查：

```text
同 conversation ordering violation = 0
duplicate final commits = 0
cross-user leakage = 0
lost durable runs = 0
```

---

# 50. 必须做故障测试

这甚至比单纯 QPS 更有说服力。

测试：

```text
Worker 正执行到一半
docker kill worker
```

期待：

```text
lease timeout
↓
run recovered
↓
latest checkpoint resume
```

再：

```text
Redis shutdown
```

期待：

```text
conversation/message/run 仍能写入 PG
worker fallback polling 仍能执行
SSE realtime 暂时 degraded
```

再：

```text
kill API instance
```

期待：

```text
run 不受影响
新 API replica 可以 GET run
SSE reconnect 恢复当前状态
```

这才是真正证明：

> durable / distributed

---

# 51. Sandbox 安全测试也要进入测试集

例如 Agent 执行：

```bash
cat /etc/shadow
```

不能得到 host 的。

执行：

```bash
curl http://postgres:5432
```

应该被阻止。

执行：

```bash
curl http://169.254.169.254
```

应该被阻止。

执行：

```bash
fork bomb
```

应该被 pids/cgroup 控制。

执行超长程序：

```text
CPU / memory / timeout
```

必须终止。

这类测试对 Agent 岗位会非常亮眼。

---

# 52. 最实际的重构顺序

我不建议“一口气重构 Cloud”。

### Phase 0 — 锁定 Local 语义

先把当前：

```text
session append-only
same-session serialization
tool execution
memory retrieval
MCP
scheduler
```

全部补成 contract tests。

Cloud 改造不能破坏它们。

---

### Phase 1 — 建立真正的 Ports

新增：

```text
TranscriptStore
RunStore
CheckpointStore
RunEventSink
ExecutionBackend
ObjectStore
SecretStore
```

现有实现全部包装：

```text
SQLiteTranscriptStore
LocalExecutionBackend
LocalObjectStore
```

此时系统行为**完全不变**。

---

### Phase 2 — 解耦 Turn 与 Channel

加入：

```text
TurnRequest
TurnResult
AgentPrincipal
```

让：

```text
PassiveTurnPipeline
```

不再依赖必须从 `InboundMessage` 发起。

Local MessageBus 写 adapter。

此阶段之后：

```text
Agent core 可以被任意 backend 调用
```

这是关键里程碑。

---

### Phase 3 — PostgreSQL + FastAPI + Auth

增加：

```text
users
auth_sessions
conversations
messages
runs
```

完成：

```text
login
conversation CRUD
message submission
```

这时 Agent 可以暂时还是单 worker。

第一个真正 Cloud MVP 出现。

---

### Phase 4 — Worker / Queue / Redis / SSE

拆：

```text
api
worker
```

实现：

```text
queued run
SKIP LOCKED admission
per-conversation serialization
Redis wakeup
Redis PubSub
SSE
cancel
lease/heartbeat
```

这一阶段做完，你的项目已经能算相当不错的 Agent Backend 项目了。

---

### Phase 5 — Durable ReAct

增加：

```text
run_checkpoint
tool_call state
retry/recovery
crash resume
external side-effect policy
```

这是从：

> 普通异步 Chat Backend

跃迁到：

> Agent Infrastructure

的一步。

---

### Phase 6 — Sandbox

把：

```text
bash
read_file
write_file
git
stdio MCP
```

全部从 host execution 切到：

```text
ExecutionBackend
```

实现：

```text
LocalExecutionBackend
SandboxExecutionBackend
```

然后加入 rootless Docker/gVisor/security limits。

这是另一个极强的项目亮点。

---

### Phase 7 — Object Storage + Cloud Memory

加入：

```text
MinIO/S3
presigned upload
artifact persistence

PostgresMemoryStore
pgvector
user-global memory
```

保留现有 MemoryEngine 算法。

---

### Phase 8 — Scheduler + Telegram

Cloud Scheduler 改为：

```text
scheduled_tasks
↓
normal Run
```

Telegram：

```text
channel identity mapping
↓
normal Conversation / Run
```

证明 multi-channel abstraction 没有因为 Cloud 化消失。

---

### Phase 9 — Hardening

加入：

```text
RLS
rate limits
encrypted credential
SSRF proxy
OpenTelemetry
Prometheus
load test
chaos test
```

---

### Phase 10 — 可选 RabbitMQ

只有在你想继续学习真正的 MQ，或者 load test 证明现有 queue hot path 值得拆时：

```text
RunStore
  ↓
Outbox
  ↓
RabbitMQ
  ↓
Worker wakeup
```

这一步才合理。

---

# 53. 我会明确写下的 ADR

为了让整个 repo 看起来像真正工程项目，我建议在：

```text
docs/decisions/
```

增加这些 Architecture Decision Records：

| ADR | 决策                                                |
| --- | ------------------------------------------------- |
| 001 | PostgreSQL 是 Cloud durable state authority        |
| 002 | Redis 只负责 ephemeral coordination                  |
| 003 | Local SQLite implementation 保留                    |
| 004 | Browser authentication 使用 opaque HttpOnly session |
| 005 | Agent execution 与 HTTP request 解耦为 Run            |
| 006 | 同 conversation 严格串行                               |
| 007 | SSE 用于 agent output streaming                     |
| 008 | Tool execution 通过 ExecutionBackend                |
| 009 | Cloud shell/stdIO MCP 必须 sandbox                  |
| 010 | Cloud V1 禁止 arbitrary Python plugin               |
| 011 | Files 存 object storage，DB 仅 metadata              |
| 012 | Memory 默认 user-global，不引入 product workspace       |
| 013 | Cloud Scheduler 创建普通 Run                          |
| 014 | Proactive/Drift 不进入 Cloud V1                      |
| 015 | RabbitMQ 不进入 MVP，未来通过 Outbox 接入                   |

这样以后面试官问：

> “为什么没直接上 RabbitMQ？”

你不是回答：

> “我还没学。”

而是：

> “因为 V1 的 run state 已经在 PostgreSQL 中，Redis 只承担 wakeup；为了避免增加第二份 durable queue 和 dual-write，我先采用 PG admission + Redis signalling，后续如需要 broker 通过 transactional outbox 接 RabbitMQ。”

这个回答水平就完全不一样了。

---

# 54. 最后，把技术栈正式定下来

| 层                 | 最终选择                        | 原因                                    |
| ----------------- | --------------------------- | ------------------------------------- |
| Language          | Python 3.11+                | 与现有项目一致                               |
| API               | FastAPI                     | async / typed / Python Agent 生态       |
| Validation        | Pydantic                    | FastAPI 原生生态                          |
| ORM               | SQLAlchemy 2 async          | 成熟 async ORM                          |
| PostgreSQL driver | Psycopg 3 async             | SQLAlchemy 官方 sync/async dialect      |
| Migration         | Alembic                     | SQLAlchemy 标准迁移体系                     |
| Durable DB        | PostgreSQL                  | 多实例 transaction / concurrency         |
| Vector            | pgvector                    | 与 durable DB 一体、初期够用                  |
| Coordination      | Redis                       | wakeup / PubSub / cancel / rate limit |
| Agent streaming   | SSE                         | server→client 单向天然匹配                  |
| Files             | S3-compatible               | durable object store                  |
| Dev object store  | MinIO                       | 本地 S3-compatible                      |
| Sandbox dev       | Rootless Docker             | 易部署、可限制资源                             |
| Sandbox target    | gVisor runsc                | hostile code isolation 更强             |
| Auth              | opaque HttpOnly session     | Browser 安全模型简单清楚                      |
| Password          | Argon2id                    | OWASP 推荐                              |
| Scheduler         | PostgreSQL-backed scheduler | APScheduler 4 尚 pre-release           |
| MQ                | V1 不使用                      | 当前无必要                                 |
| MQ advanced       | RabbitMQ + Outbox           | 真出现需要时再引入                             |
| Observability     | OpenTelemetry + Prometheus  | 标准 tracing/metrics                    |
| Deployment        | Docker Compose              | 足够展示水平扩容                              |
| Kubernetes        | 暂不使用                        | 当前属于不必要复杂度                            |

---

## 最终效果

你现在的 Kirakira 故事是：

```text
LLM
 ↓
ReAct
 ↓
Tools / MCP
 ↓
Memory
 ↓
Local Agent Runtime
```

重构完成之后会成为：

```text
                  ┌─ Local SQLite
                  ├─ Local Shell
                  │
Agent Runtime ────┤
                  │
                  ├─ Multi-user Cloud API
                  ├─ PostgreSQL durable state
                  ├─ Redis coordination
                  ├─ Distributed Workers
                  ├─ Durable ReAct checkpoints
                  ├─ SSE streaming
                  ├─ Sandbox execution
                  ├─ S3 object storage
                  ├─ pgvector memory
                  └─ multi-channel adapters
```

最重要的提升不是技术名词数量，而是你能完整解释一条工程演化链：

> **单用户 local runtime → 身份与持久化 → async Run → worker/backpressure → 分布式并发控制 → crash recovery → durable Agent execution → sandbox isolation → horizontal scaling。**

而且你现有仓库已经有很好的起点：Session 是 append-only truth，MessageBus 有 ordering，ToolRegistry 已经有 risk metadata，Memory 已经有 engine protocol，`ports.py` 已经主动建立依赖注入缝。

所以**第一刀不应该是 FastAPI，也不是 PostgreSQL**。

第一刀应该是：

> **把 `PassiveTurnPipeline / SessionManager / Tool execution` 与“本地 workspace + channel + asyncio bus”真正解耦。**

只要这个 seam 做对，后面 PostgreSQL、Worker、Redis、Sandbox 都是在往 adapter 里填实现，而不是越来越多地把 `if cloud:` 塞进现有 Agent Runtime。

如果按代码落地，我会从 **Phase 0～Phase 2** 开始，不碰任何 Cloud 基础设施，先把整个仓库重构成“能够被 Cloud backend 调用、但现有 Local 行为完全不变”的状态。这会决定后面整项工程是越来越干净，还是变成 Local/Cloud 两套逻辑纠缠在一起。

[1]: https://docs.openhands.dev/sdk/api-reference/openhands.sdk.workspace "https://docs.openhands.dev/sdk/api-reference/openhands.sdk.workspace"
[2]: https://docs.langchain.com/langsmith/agent-server "https://docs.langchain.com/langsmith/agent-server"
[3]: https://redis.io/docs/latest/develop/pubsub/ "https://redis.io/docs/latest/develop/pubsub/"
[4]: https://docs.langchain.com/langsmith/data-plane "https://docs.langchain.com/langsmith/data-plane"
[5]: https://docs.openwebui.com/getting-started/advanced-topics/scaling/ "https://docs.openwebui.com/getting-started/advanced-topics/scaling/"
[6]: https://fastapi.tiangolo.com/tutorial/background-tasks/ "https://fastapi.tiangolo.com/tutorial/background-tasks/"
[7]: https://docs.langchain.com/langsmith/agent-server-scale "https://docs.langchain.com/langsmith/agent-server-scale"
[8]: https://www.postgresql.org/docs/18/sql-select.html "https://www.postgresql.org/docs/18/sql-select.html"
[9]: https://docs.langchain.com/langsmith/agent-server-changelog "https://docs.langchain.com/langsmith/agent-server-changelog"
[10]: https://microservices.io/patterns/data/transactional-outbox.html?source=post_page-----fa3de00ceba5--------------------------------------- "https://microservices.io/patterns/data/transactional-outbox.html?source=post_page-----fa3de00ceba5---------------------------------------"
[11]: https://blog.rabbitmq.com/docs/4.1/quorum-queues "https://blog.rabbitmq.com/docs/4.1/quorum-queues"
[12]: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events "https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events"
[13]: https://docs.openhands.dev/sdk/arch/workspace "https://docs.openhands.dev/sdk/arch/workspace"
[14]: https://docs.docker.com/engine/security/rootless/ "https://docs.docker.com/engine/security/rootless/"
[15]: https://docs.docker.com/engine/security/seccomp/ "https://docs.docker.com/engine/security/seccomp/"
[16]: https://gvisor.dev/docs/architecture_guide/security/ "https://gvisor.dev/docs/architecture_guide/security/"
[17]: https://github.com/langgenius/dify/blob/main/docker/docker-compose-template.yaml "https://github.com/langgenius/dify/blob/main/docker/docker-compose-template.yaml"
[18]: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports "https://modelcontextprotocol.io/specification/2025-06-18/basic/transports"
[19]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html "https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html"
[20]: https://github.com/pgvector/pgvector "https://github.com/pgvector/pgvector"
[21]: https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html "https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html"
[22]: https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html "https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html"
[23]: https://www.postgresql.org/docs/current/ddl-rowsecurity.html "https://www.postgresql.org/docs/current/ddl-rowsecurity.html"
[24]: https://docs.sqlalchemy.org/en/20/dialects/postgresql.html "https://docs.sqlalchemy.org/en/20/dialects/postgresql.html"
[25]: https://docs.sqlalchemy.org/en/21/orm/extensions/asyncio.html "https://docs.sqlalchemy.org/en/21/orm/extensions/asyncio.html"
[26]: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html "https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html"
[27]: https://redis.io/docs/latest/develop/use-cases/rate-limiter/ "https://redis.io/docs/latest/develop/use-cases/rate-limiter/"
[28]: https://apscheduler.readthedocs.io/en/master/versionhistory.html "https://apscheduler.readthedocs.io/en/master/versionhistory.html"
[29]: https://fastapi.tiangolo.com/deployment/docker/ "https://fastapi.tiangolo.com/deployment/docker/"
[30]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html "https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html"
