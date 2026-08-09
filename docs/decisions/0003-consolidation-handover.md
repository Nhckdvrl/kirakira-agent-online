# 0003 Consolidation 移交给 MarkdownMemoryMaintenance

- 状态：accepted
- supersedes：[0002](./0002-consolidation-driver.md)

## 决定

`MarkdownMemoryMaintenance` 订阅 `TurnCommitted`，成为 Markdown 归档和近期上下文刷新的唯一驱动方。
turn 开始前也等待同一个 maintenance 收口，不再等待已退役的 MemoryRuntime 任务集。

context guard 直接调用可等待的 `maintenance.consolidate(force=True)`，并与后台维护共用 session lock。
归档无法推进且历史超过保护阈值时拒绝本轮，避免静默丢历史。

## 移交清单

- 触发归档；
- 按 session 串行；
- 推进并保存 consolidation cursor；
- 下一轮前等待上一轮收口；
- 超时不取消正在进行的归档；
- 失败按“未推进”处理，不把半完成状态当成功。

## 明确变化

旧路径顺带写入的 `HISTORY.md` 审计追加已停止，因为运行时没有读取方。没有 embedding 时，旧的
“请记住”正则自动抽取也不再存在；用户仍可用 `memorize` 显式写入。保留这些说明是为了避免把职责移交
误解为完全无行为变化。
