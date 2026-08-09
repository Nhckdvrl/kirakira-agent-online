# 0002 Consolidation 暂由 MemoryRuntime 驱动

- 状态：superseded by [0003](./0003-consolidation-handover.md)

## 历史背景

早期 `MarkdownMemoryMaintenance` 尚未接线，而 DefaultMemoryEngine 已监听 consolidation 事件。为了先让
结构化长期事实提取通电，当时保留 `MemoryRuntime` 作为唯一归档驱动方，并在归档后广播事件。

## 当时的理由

两条路径同时订阅 `TurnCommitted` 会重复归档并竞争游标；旧 runtime 还承载了“归档无法推进时拒绝继续
扩张上下文”的保护。直接切换会同时丢掉去重和 guard。

## 后续

上述职责已经完整移交给 `MarkdownMemoryMaintenance`，旧驱动路径已删除。当前合同以
[0003](./0003-consolidation-handover.md) 和[记忆架构](../architecture/memory.md)为准。
