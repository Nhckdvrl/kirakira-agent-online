# Workspace 迁移

`migrations/yoyo/` 是唯一自动执行的 workspace migration catalog。启动时先获取 workspace 单实例锁，
再应用未执行迁移并写入 `<workspace>/migrations.sqlite3`，之后才打开运行状态库。

## 规则

1. 已发布的 migration 文件不可修改或删除。
2. 新 migration 必须用 `__depends__` 明确依赖关系。
3. migration 应可重复检查，失败时阻止 runtime 继续打开状态。
4. `.instance.lock` 的作用域是一个 workspace，不是整个仓库或用户目录。
5. schema 演进归 migration 管，启动代码不应散落临时 ALTER 语句。

origin migration 只记录 Kirakira 当前 workspace schema，不会改写 `sessions.db`、`memory/coremem.db`、
`memory/akasha.db` 或 Markdown memory 内容。这些数据层有各自的 owner 和兼容策略。

## 验证

```bash
uv run pytest -q tests/test_migration_runner.py tests/test_yoyo_migration_append_only.py tests/test_workspace.py
```

这些测试覆盖 migration runner、append-only 目录合同和 workspace 初始化/锁边界。
