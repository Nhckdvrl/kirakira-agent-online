# 使用记忆系统

## 选择引擎

```toml
[memory]
enabled = true
plugin = "default" # 或 akasha

[memory.embedding]
model = "text-embedding-v3"
base_url = "${EMBEDDING_BASE_URL}"
api_key = "${EMBEDDING_API_KEY}"
```

聊天 completion 端点通常不提供 embedding，需要单独配置。

## Default 和 Akasha 的区别

| 引擎 | 适合什么 | 写入方式 | 工具 |
| --- | --- | --- | --- |
| `default` | 稳定事实、偏好、流程、事件 | 从对话抽取结构化 item，也可显式写入 | memorize / recall / forget |
| `akasha` | 保留完整对话关系和联想 | `TurnCommitted` 后自动摄入完整 turn | recall / reinforce |

Akasha v1 当前可用，已通过真实 embedding、turn 摄入、图/RAR 召回和模型消费验证。它没有
`memorize` 是设计结果，不是工具漏注册。

## 三类数据不要混淆

- `sessions.db/messages`：原始对话真相。
- `memory/coremem.db` 或 `memory/akasha.db`：长期记忆引擎状态。
- `MEMORY.md`、`SELF.md`、`PENDING.md`、`RECENT_CONTEXT.md`：Markdown 档案。

删除或压缩模型上下文不能直接删除这些数据。Session 删除和长期记忆遗忘必须走各自明确操作。

## 管理命令

```bash
uv run python main.py memory doctor
uv run python main.py memory backup
uv run python main.py memory migrate
uv run python main.py memory verify
uv run python main.py memory rollback --backup-id <id>
uv run python main.py memory repair-kinds --dry-run
```

`repair-kinds` 把旧的 `identity/fact/requested_memory` 归一为可注入的 `profile`。正式执行前会备份；
先用 `--dry-run` 查看范围。

## 失败语义

| 情况 | 行为 |
| --- | --- |
| 查询 embedding 失败 | 可降级关键词 lane，并在 trace 中标明 |
| 写入 embedding 失败 | 抛错，不发布半索引记录 |
| SQLite integrity/schema 异常 | fail loud，不用旧 JSON 覆盖恢复 |
| consolidation 模型失败 | 回复保留；记录失败，不伪造记忆成功 |
| 普通 forget | 逻辑退休；物理删除只允许明确管理操作 |

内部结构见 [记忆架构](../architecture/memory.md)，评测边界见
[记忆评测](../operations/memory-evaluation.md)。
