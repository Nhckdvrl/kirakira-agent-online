# 验证记录

更新日期：2026-08-12。这里只记录实际执行证据，不用测试数量代替功能范围。

## 自动化回归

- 全量快速测试：`634 passed, 1 skipped, 22 warnings, 4 subtests passed`。
- 覆盖认证/隔离、conversation/Run 事务、lease/heartbeat/reaper/cancel、SSE、rate limit、Origin、
  idempotency、checkpoint、remote sandbox contract、Memory、Proactive、Drift 和原算法回归。
- opt-in PostgreSQL 测试未配置连接串时是唯一 skip；SQLite 只承担快速合同测试。

## Neon PostgreSQL 实测

项目 `young-shape-58872656` 的临时分支 `br-old-meadow-avm7rl3x` 已成功执行 Alembic
`20260809_03 → 20260809_13`：

- Alembic head 为 `20260809_13`；
- pgvector extension 与 1024 维 HNSW index 存在；
- automation/inbox、Run event、rate limit、worker、checkpoint、Memory profile、Proactive/Drift 表存在；
- message automation delivery unique constraint 存在。

真实应用集成测试结果 `1 passed`：

- 12 个 worker 并发 claim 同一 automation，只有一个 owner；
- 30 个并发固定窗口请求、limit=10，精确放行 10 个；
- 20 条同 conversation 并发消息得到连续且不重复的 seq；
- 1024 维 embedding 同时写入 JSON 与 pgvector，和自身的 cosine distance 为 0。

2026-08-12 已将同一份 migration SQL 提交到 main branch `br-orange-band-aviu5anr`，并由 Neon 删除
临时分支。提交后只读复核结果：

- `alembic_version = 20260809_13`；
- `vector = 0.8.1`；
- `agent_automations`、`automation_inbox_events`、`run_events`、Memory profile/replacement 表存在；
- automation due/lease/inbox 索引、Run event sequence unique constraint、message delivery unique
  constraint 与 `memory_items` 1024 维 HNSW index 存在。

迁移 SQL SHA-256：`34f6e545d200c686080857a1dc47c8b7cb7ae7f78a54866bd1581445738338f2`。

## 常用发布检查

```bash
uv run pytest -q
uv build
KIRAKIRA_TEST_POSTGRES_URL='postgresql://…' \
  uv run pytest -q tests/test_cloud_postgres_integration.py
```

真实 sandbox、模型/embedding credential、systemd 安装和目标容量压测属于部署环境验收，不能用 mock
测试冒充。Cloud readiness 会在 sandbox 未证明隔离能力时拒绝 worker 启动。
