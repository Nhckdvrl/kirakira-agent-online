# Kirakira Online：产品方案、评审与实施记录

更新日期：2026-08-12。

这一个文件同时保存原始需求、方案评审、最终架构、功能迁移矩阵和实施状态。它取代此前分散且重复的阶段
稿；Git 历史保留过程，本文件只描述当前事实。

## 1. 原始需求与结论

原项目是单机 Agent harness：用户在本地安装依赖，配置模型与 embedding API key，再由一个进程持有
会话、Memory、工具、Scheduler、Plugin、Proactive 和 Drift。目标不是把它包装成一个公网接口，而是把它
重构成可供多人长期使用、能够扩容和恢复的在线产品，同时保留原 Agent 能力与算法。

最终产品形态已经确定为：

- 浏览器、Telegram、OneBot QQ 和腾讯官方 QQBot 是客户端；
- API 只处理认证、准入、配置和 durable state，不在 HTTP 请求中跑 Agent；
- PostgreSQL 是用户、会话、消息、Run、Memory、调度和集成配置的真相源；
- 多个 worker 通过数据库 claim、lease 和 heartbeat 横向竞争；
- 模型和 embedding key 是服务端配置，不交给浏览器；
- shell、文件和附件只进入独立 Bubblewrap sandbox，绝不在 API/worker 宿主机执行；
- Local TUI/setup/bootstrap 不再是发行产品，也没有公开命令；旧源码只作为原算法与合同回归夹具。

这不是“另写一套简化 Agent”。Cloud worker 继续调用原 `PassiveTurnPipeline`、`DefaultReasoner`、ReAct、
query compaction、Default Memory、Proactive 和 Drift 实现；变化集中在身份、存储、调度、投递和隔离边界。

## 2. 产品与工程评审

### 2.1 为什么不能只做一个 Agent API

1000 个用户同时使用时，问题不是把 LLM 调用包进 FastAPI：

- 每个请求必须先确定 user、conversation、message 和 Run，任何查询都不能跨租户；
- 同一 conversation 的两轮不能交错改写历史，不同 conversation 又必须可以并行；
- worker 崩溃后，尚未完成的 Run、Scheduler、渠道投递和后台 Agent 必须能重新发现；
- 对外部系统有副作用的工具不能在崩溃后盲目重放；
- SSE 断线、渠道重复 webhook、浏览器重试不能制造重复消息；
- 用户文件和 shell 不能共享宿主机目录或进程；
- Plugin/MCP/Skill 不能成为跨用户泄密或任意 Python 代码进入主进程的入口。

因此中间件的选择来自一致性问题，而不是为了展示技术名词。PostgreSQL 同时承担 durable state、行锁、
队列 claim、幂等约束和租约；Redis 可以以后作为 wakeup/cache 优化，但不承担正确性，所以暂不强制引入。
RabbitMQ 也不是必需项：当前 Run 和后台任务都能从 PostgreSQL 重发现，先引入 MQ 反而会增加双写真相源。

### 2.2 对成熟产品形态的映射

调研得到的共同模式是“同一 Agent 语义，多种执行/部署边界”：LangGraph 区分本地开发和 Agent Server，
OpenHands 用统一 workspace 接口切换执行环境，云端 coding agent 把不可信执行放在独立 sandbox。Kirakira
采用相同原则，但正式发行只暴露 Cloud 进程。

参考资料：

- [LangGraph local development](https://docs.langchain.com/langsmith/local-dev-testing)
- [LangSmith deployment](https://docs.langchain.com/langsmith/deployments)
- [OpenHands runtime architecture](https://docs.openhands.dev/modules/usage/architecture/runtime)
- [PostgreSQL `SKIP LOCKED`](https://www.postgresql.org/docs/current/sql-select.html)
- [OWASP Session Management](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)
- [MCP specification](https://modelcontextprotocol.io/specification/2025-03-26)

### 2.3 评审后的关键决定

| 决定 | 原因 |
| --- | --- |
| opaque HttpOnly cookie，不把 token 放 localStorage | 降低浏览器凭据泄漏面；服务端 session 可撤销 |
| PostgreSQL queue + lease | 一套 durable truth，支持 `SKIP LOCKED` 水平扩容和崩溃恢复 |
| 同 conversation 串行、跨 conversation 并行 | 保住上下文顺序，同时让多用户并发 |
| 工具 checkpoint | 已完成可 replay；只有 started 的外部副作用槽位必须 fail closed |
| pgvector 只召回候选 | 原 Python Memory 评分仍是最终权威，不让 ANN 改算法语义 |
| Proactive/Drift 自动 tick | 用户开关是能力配置，不是“点击触发主动算法” |
| Remote Plugin Service | 保留扩展点，同时禁止租户任意 Python 进入共享 worker |
| Bubblewrap 独立服务 | 无 Docker 条件下仍提供真实 namespace 隔离和默认断网 |
| 不提供 Local 产品命令 | 仓库作为独立在线版发布；旧 adapter 只为回归测试 |

## 3. 最终运行架构

```text
Browser / Channel
  └─ FastAPI / Channel Gateway
       ├─ auth + tenant predicate + rate limit
       ├─ Message + queued Run transaction
       └─ PostgreSQL durable state
            ├─ passive Run workers
            ├─ Proactive / Drift automation worker
            ├─ Scheduler worker
            ├─ Plugin job/source worker
            ├─ Subagent worker
            └─ Channel delivery outbox worker

Cloud worker
  └─ original Agent pipeline
       ├─ context / skill / Memory retrieval
       ├─ ReAct + query compaction
       ├─ built-in tools + per-user MCP + per-user Plugin snapshot
       ├─ remote Bubblewrap sandbox
       └─ assistant Message + RunEvent + channel outbox
```

### 3.1 多用户数据边界

`User → Conversation → Message → Run → RunEvent` 是被动对话主链。Conversation 行锁分配连续 seq；同一
conversation 最多一个 running Run；幂等键以 user 为作用域。Memory、profile、Proactive、Drift、文件、
Scheduler、渠道、MCP、Plugin、Skill 和子代理表都带 user 外键或必须在 task-local user binding 下访问。

API 的每个资源查询都带已认证 `user_id`。远程工具能力在 turn 开始时编译为 snapshot 并持有 lease；同一
turn 的 tools、phase、hook、MCP 连接和用户 Skill 不会中途换代，也不会被其他用户看到。

### 3.2 Run、恢复与背压

消息准入在一个事务中写 user Message、queued Run 和 `run.queued` event。worker 使用
`FOR UPDATE SKIP LOCKED` claim，随后持续 heartbeat；lease 过期后 reaper 可重排。取消请求和 lease 丢失
都会阻止陈旧 worker 提交最终结果。

RunEvent 有单调 seq。SSE 接受 `Last-Event-ID`，文本 delta 先小批聚合再 durable 写入。工具调用在执行前写
checkpoint：成功或错误结果可以在恢复时 replay；如果外部副作用工具只有 started 没有结果，则终止 Run
并要求人工确认，不能假设“没执行”。

### 3.3 Sandbox 与文件

`kirakira-sandbox` 是仓库内真实服务，不再只是 client contract。它使用 Bubblewrap：

- `--unshare-all`，网络默认 deny；
- `/usr`、`/bin` 和运行库只读；
- 每个 conversation 使用哈希 owner workspace；
- workspace API 拒绝绝对路径、`..` 和 symlink 穿越；
- 支持 shell、PTY/stdin、后台任务、终止 owner、文本/二进制读写和 edit；
- API/worker 启动时验证 capability attestation，不满足隔离即拒绝启动。

浏览器和 Telegram/QQ 入站附件先写 sandbox，再把 metadata 写 PostgreSQL。vision、read/write/edit、bash 和
渠道附件投递都使用同一 workspace authority。

## 4. 原功能等价迁移矩阵

| 原 Kirakira 能力 | Online 实现 | 语义状态 |
| --- | --- | --- |
| 多轮对话与历史 | Conversation/Message/Run + transcript hydration | 保留，增加多租户隔离 |
| ReAct/tool loop | 原 `DefaultReasoner` 与 tool executor | 原实现复用 |
| context projection/retry | 原 ContextBuilder、预算与分级 retry | 原实现复用 |
| query compaction | 原 soft/hard limit、摘要与 persistence payload | 原实现复用 |
| Memory memorize/recall/forget | 原 Default Memory engine + user-scoped PostgreSQL store | 原公式复用 |
| Markdown profile/consolidation | user-scoped profile tables + 原 maintenance | 原实现复用 |
| pgvector | 1024 维 HNSW candidate recall | 只优化候选，不替换评分 |
| Proactive | durable inbox/tick/decision/delivery + 原 energy/gate/judge DAG | 原算法复用 |
| Drift | durable schedule/journal/continuum/run + 原 hazard/fairness/drive | 原数学公式复用 |
| Scheduler | `at/after/every`、cron、timezone、instant/soft、misfire | 原解析与触发语义复用 |
| bash/PTY/background/stdin | Bubblewrap sandbox service | 保留，隔离增强 |
| 文件/vision | conversation workspace + upload/download metadata | 保留，隔离增强 |
| `spawn/spawn_manage` | durable subagent jobs、每用户限额、lease、取消、完成回注 | 保留，恢复增强 |
| `message_push` | 只允许当前用户已配对渠道，durable outbox | 保留，权限增强 |
| Telegram | 原生 Bot API polling、pairing、文本/图片/文件、outbox | 保留 |
| OneBot QQ | 原生 webhook、私聊/群聊 target、CQ 图片/文件发送 | 保留 |
| 腾讯 QQBot | 原生 token、Gateway、heartbeat、C2C 与 reply id | 保留；与原版一致不支持附件 |
| MCP | per-user HTTPS Streamable HTTP、加密 headers、turn snapshot | 保留，禁用本地进程 MCP |
| Plugin lifecycle | remote service 的 7 phases、tools、tool hooks、slot/priority | 保留扩展点 |
| Plugin jobs/LLM | durable interval task、最多 5 个受限 Cloud LLM request | 保留且可恢复 |
| Plugin Proactive source | durable source polling → canonical inbox | 保留 |
| Plugin MCP | manifest 声明的 remote MCP 并入同一 snapshot | 保留 |
| Skill catalog/`$skill`/always/`load_skill` | per-user durable Skill + task-local SkillLoader overlay | 保留且不串租户 |
| `/tools` `/skills` `/memory` | 读取当前 user snapshot/Skill/Cloud Memory | 保留 |

这里的“保留”指用户可观察语义，不指照搬不适合共享服务的执行方式。唯一刻意取消的是“把任意租户 Python
插件或 shell 直接放进 Agent 宿主进程”：前者改成 remote Plugin Service，后者改进 Bubblewrap；否则无法
声称这是多用户在线产品。

## 5. Proactive 与 Drift：没有简化公式

用户配置 `proactive_enabled` / `drift_enabled` 只决定后台能力是否运行，以及选择哪个 conversation 接收
结果。实际是否发送、何时运行，仍由 worker 自动 tick 和原算法决定。

Cloud 没有把 Proactive 改成固定 cron prompt，也没有把 Drift 改成随机消息：

- Proactive 继续执行 source fetch/ack、energy、gate、judge、delivery 和 feedback；
- 只有 Proactive 没有可投递内容时才进入 Drift；
- Drift 继续执行 timer/hazard sampling、skill 公平选择、journal/continuum、briefing 和 drive；
- 原测试中的概率、时间衰减、hazard、冷却、公平性与 journal 约束仍由同一 Python 模块承重；
- Cloud adapter 只把原文件状态换成带 user scope 的 PostgreSQL store，并增加 lease/fencing/idempotency。

因此“开启 Proactive/Drift”不是用户手动触发一次计算，而是授权这个 conversation 成为长期在线的主动
Agent 目标。被动用户消息优先，会撤销同会话 automation lease；失去 lease 的后台任务不得提交。

## 6. 在线扩展协议

### 6.1 MCP

用户在 Web/API 保存名称、公开 HTTPS base URL 和 headers。headers 用 Fernet 加密，响应、日志、对话与
manifest 都不回显。URL 禁止 userinfo、私有/loopback/link-local/reserved DNS；可配置域名 allowlist。
每轮完成 initialize、initialized、tools/list，工具以 deferred capability 加入 snapshot。

### 6.2 Plugin Service

`GET /v1/manifest` 返回 capability declaration。支持：

- `phases`: 七个 phase；字符串或带 `priority`、`slot`、`requires` 的对象；
- `tools`: schema、always_on、risk；
- `tool_hooks`: pre/post/error；
- `jobs` / `sources`: durable interval task；
- `mcp_servers`: 由插件服务提供的 remote MCP；
- job/source 返回 `events` 可进入 canonical Proactive inbox；
- job 可返回受限 `llm_requests`，worker 调用服务端模型后回调 `/complete`。

phase 只能 patch 已存在的 context 字段，不能修改 session/user/channel/timestamp 身份。工具继续经过原 schema
validation、loop guard 和 checkpoint。服务 URL 与凭据遵循和 MCP 相同的 SSRF/加密规则。

## 7. Web 产品面与 API

仓库自带同源 Web UI：注册/登录、多个 conversation、历史、附件、Run 状态、Proactive/Drift 配置、
Scheduler、渠道配对、MCP、Plugin 和用户 Skill 管理。UI 使用 HttpOnly cookie，不在 localStorage 保存会话。

主要 API 组：

```text
/v1/auth/*                    auth session
/v1/conversations/*           conversation, message, file, automation, schedule
/v1/runs/*                    Run state, cancel, durable event/SSE
/v1/channel-links             paired Telegram/QQ/QQBot identities
/v1/mcp-servers               per-user MCP
/v1/plugins                   per-user remote Plugin Service
/v1/skills                    per-user Skill
/v1/subagents                 durable child jobs
/healthz /readyz /metrics     operations
```

## 8. 进程、部署和容量

发行包只有四个命令：

```text
kirakira-cloud-api
kirakira-cloud-worker
kirakira-sandbox
kirakira-channel-gateway
```

`deploy/systemd/` 提供四类加固 unit；worker 使用 template unit 扩容。API 和普通 worker 可以水平扩展；
Telegram/QQBot 接收 gateway 应保持单 active consumer，渠道 outbox 本身仍用 lease。生产环境应把 API、
sandbox 和 gateway 放在反向代理/TLS 后，sandbox/gateway 使用独立长 service token。

1000 个用户不是“启动 1000 个 Agent 进程”。用户请求先进入短事务，worker 数量按模型 provider 配额、
平均 Run 时长和工具耗时设置。PostgreSQL pool 必须按 API/worker 实例总数预算；队列深度、claim latency、
Run duration、lease recovery、模型 429 和 sandbox saturation 是压测指标。

## 9. 实施与验证状态

当前 Alembic head 是 `20260812_20`。2026-08-12 已通过 Neon 安全迁移流程把 `_14 → _20` 应用到项目
`young-shape-58872656` 的 main `br-orange-band-aviu5anr`；临时分支验证后已删除。main 只读复核显示 head
正确且 11 张新增表全部存在。

当前自动化证据：

- `661 passed, 1 skipped, 23 warnings, 4 subtests passed`；
- wheel/sdist 构建成功，静态 UI、`_20` migration、sandbox 和四个在线 entrypoint 都在 wheel；
- JavaScript syntax、`git diff --check` 和单 Alembic head 检查通过；
- Bubblewrap 在当前 Linux 主机完成真实 namespace command 验证；
- Neon 临时分支验证了 11 张表、due/lease/claim 索引、unique/check/FK cascade 与 head。

唯一 skip 是需要调用方显式提供 PostgreSQL URL 的 opt-in 集成测试；此前 Neon 已真实执行该测试覆盖的
`SKIP LOCKED`、并发限流、conversation seq 和 pgvector 行为。详见[验证记录](../operations/verification.md)。

## 10. 仍需部署方确认

代码迁移没有待确认项。上线环境仍必须由部署方提供或决定：

- 正式域名、TLS/反向代理和 `KIRAKIRA_ALLOWED_ORIGINS`；
- 模型、embedding、Telegram/QQ/QQBot 和 remote integration credentials；
- 数据保留/删除周期、备份/PITR、预算与供应商配额；
- 目标并发、p95/p99 SLO 和据此执行的容量压测；
- sandbox 主机的 OS patch、磁盘 quota、CPU/memory/cgroup 限额和审计策略。

这些是实际部署环境与产品运营决定，不是用 Local fallback 掩盖的代码缺口。缺少 PostgreSQL、模型、
embedding、credential key 或合格 sandbox 时，Cloud 服务会 fail closed。
