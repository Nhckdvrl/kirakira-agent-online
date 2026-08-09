"""控制面:让外部程序观测与驱动运行中的 agent。

对齐 Reference `agent/control/` + `infra/control/`:领域模型与状态机在
`models.py`/`store.py`,turn 编排在 `runtime.py`,协议投影在 `service.py`,
JSON-RPC 2.0 over NDJSON 在 `protocol/` 与 `socket.py`。
"""
