# 工具、Shell、子 Agent 与 Scheduler

## ToolMeta 与 deferred 工具

每个工具除了 JSON schema，还带有 `risk`、`always_on`、`preloadable`、`search_hint` 和来源
（builtin/MCP/plugin）。核心工具默认可见；较重的 MCP/plugin 工具可以 deferred。

模型通过 `tool_search(query="select:工具名")` 解锁 deferred 工具。每个 Session 只保留有限 LRU，
避免工具 schema 长期占满上下文。

## Shell

`bash` 使用统一执行管理器：

- 前台或后台；
- PTY 与普通 pipe；
- `shell/login` 选项；
- 首次 yield 和输出 token 上限；
- timeout、取消和进程组清理。

后台执行返回 `execution_id`。使用：

- `write_stdin`：向 PTY/进程输入字符并获取新输出；
- `task_output`：兼容方式读取增量输出；
- `task_stop`：停止并清理执行。

执行归属于当前 turn 或子 Agent owner。清理一个 owner 不能误杀其他 Session 的任务。Runtime 关闭时
会清理所有剩余进程组。

## 子 Agent

`spawn` 支持 inline 和 background。每个 child 使用独立 Session 与执行 owner；inline/background
共用全局容量上限。完成后返回结构化状态并释放 Shell/MCP 等资源。

权限 profile 控制 child 可用工具。新注册工具不会自动绕过 profile；高风险工具仍需显式开放。

## Scheduler

`schedule` 支持：

- `at`：指定时间；
- `after`：相对时长；
- `every`：interval 或 5/6 段 cron；
- IANA timezone；
- `instant` 与 `soft` tier。

`instant` 直接执行任务；`soft` 通过隔离 `scheduler:<job-id>` Session 发起 turn。soft turn 默认跳过
普通记忆检索、回复后记忆写入和消息推送工具，避免污染用户对话。

管理工具：`list_schedules`、`cancel_schedule`。持久任务有容量上限；cron/interval 会计算下一次
运行时间，离线错过 one-shot 超过 grace period 时标记 `missed`。

## 常见问题

- 后台命令找不到：使用同一个 `execution_id`，不要把 shell PID 当 execution id。
- PTY 没响应：通过 `write_stdin` 发送换行或控制字符，并检查 `process_status`。
- child 结束后进程还在：这是 owner cleanup 回归，应检查统一执行管理器，而不是再加一套 kill 逻辑。
- 定时任务污染聊天：确认 tier 是 `soft` 且 Session key 为 `scheduler:<job-id>`。
