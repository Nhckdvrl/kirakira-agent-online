# Proactive 与 Drift

两者都是后台自动触发的主动能力。用户不会手工触发 tick；用户只选择主动 Agent conversation，独立开关
用于高级设置和运维降级。

## Proactive

外部 webhook/feed/MCP adapter 先把 alert 或 content 写入 conversation 对应的 durable inbox。worker 到期
领取 schedule 后仍执行原模块链：

```text
Gate → Fetch → Ingest → JudgeContext → Alert → Content → Drift
```

energy、近期 presence、基础分、tick jitter、cooldown、内容排序、兴趣 Memory 检索、LLM judge、ACK、
feedback 和 delivery dedup 都保留原实现。API 写入 inbox 只代表“数据源出现新事件”，不代表一定推送。

投递前先持久化 fingerprint；数据库确认写入目标 Conversation 后才 consume、cooldown 和记录 push。
automation 的 tick token 同时作为消息幂等键，因此 worker 在提交响应后崩溃并重试不会生成第二条消息。

## Drift

只有 Proactive 本轮没有消息可投递时才进入 Drift。原数学和连续性合同保持：

- sampled hazard expiry，轮询频率不会改变触发概率；
- `min_interval_hours` 硬下限；
- 最近 user/drift 时间改变时重新采样；
- 从未执行或最久未执行的 skill 优先；
- 每步必须工具调用，最后具名强制 `finish_drift`；
- journal、self observation、scratchpad 和 next tendency durable 保存。

Cloud 只加载发行包中的审计 skills，不从用户 workspace 执行任意 Python/plugin。模型、Memory、工具仍与
被动 Agent 共用，但工具执行进入 remote sandbox。

## 并发与失败

schedule 由 PostgreSQL lease + heartbeat 保护，多 worker 只会有一个 owner。用户消息会撤销后台 lease；
每次工具调用前再次检查 lease 和 queued/running passive Run。投递事务也再次检查，因此后台消息不会插到
一个已开始的被动 Run 中。

切换主动 conversation 时旧目标停用，并清空用户级 Proactive reservoir/Drift continuity 后开始新 epoch，
避免不同 conversation 的后台上下文相互消费。Memory 仍按产品决定保持 user-global。
