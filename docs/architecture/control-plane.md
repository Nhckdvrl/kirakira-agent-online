# 控制面架构

控制面让 CLI 或其他本地客户端连接正在运行的 Agent，创建 turn、订阅事件、取消执行并读取结果。它不
绕过 AgentLoop，也不另建一套会话事实源。

## 分层

| 层 | 目录 | 职责 |
| --- | --- | --- |
| 协议模型 | `agent/control/protocol/` | 请求、响应、错误和路由 |
| 领域服务 | `agent/control/` | turn 状态机、事件、store、client |
| 传输实现 | `infra/control/` | 本地 socket 连接和 server |
| 装配与 CLI | `bootstrap/control.py`、`bootstrap/control_cli.py` | 接入 runtime、命令行投影 |

这种边界让领域层不依赖 socket 细节，传输层也不理解 Agent 业务。

## Turn 流程

```text
client request
  → protocol router 校验
  → ControlService 创建 turn
  → ConversationRuntime 调用现有 AgentLoop
  → 事件按序写入/广播
  → completed、failed 或 cancelled
```

控制面 turn 使用稳定 ID、明确状态转换和 CAS 更新，避免并发取消与完成互相覆盖。持久状态位于
`<workspace>/.kirakira/control.db`；聊天记录仍由 `sessions.db` 管理。

## 事件

事件包含 turn 生命周期、文本增量、工具调用、结果和错误。订阅者通过 cursor 继续读取，断线重连不需要
猜测最后收到哪一条。事件投影使用 AgentLoop 的真实 tool chain 和 context trace，不由客户端自行拼接。

终态只有一个：

- `completed`：AgentLoop 正常结束；
- `failed`：模型、工具或运行时错误；
- `cancelled`：取消请求成功提交。

## 错误语义

协议错误使用稳定 code，而不是把内部异常堆栈直接返回。常见错误包括参数非法、turn 不存在、状态冲突、
cursor 失效和 runtime 暂不可用。可重试错误会显式标记；客户端不应对所有错误盲目重试。

## 与重启和快照的关系

控制服务属于当前 gateway generation。Supervisor 换代时旧 generation 停止接受新请求；在途 turn 仍按
它锁定的插件/MCP 快照收口。客户端断开后可以连接新 generation，并依据持久 turn/event 状态恢复。

## 验证

```bash
uv run pytest -q tests/test_control_plane.py
uv run python main.py control --help
```

在线验证边界见[验证报告](../operations/verification.md)。
