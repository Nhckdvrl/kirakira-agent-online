# 快照、代际与租约

插件和 MCP 支持热重载，但一轮 Agent turn 或 Proactive tick 必须看到稳定的能力集合。Kirakira 用
generation、snapshot 和 lease 解决这个问题。

## 三个概念

- **Generation**：一次完整加载并校验后的插件/MCP 代际；
- **Snapshot**：某次执行可见的工具、hook、source、Channel 和 MCP 连接集合；
- **Lease**：执行期间对该 snapshot 的引用。

## 发布过程

```text
文件或配置变化
  → 构建候选 generation
  → 校验并连接全部候选资源
  → 原子发布 current generation
  → 新执行获取新 snapshot
  → 旧执行继续持有旧 lease
  → 最后一个 lease 释放后关闭旧资源
```

候选失败时不会部分发布；旧 generation 继续服务。删除全部 MCP 声明会发布空 generation，而不是把
旧连接无限保留。

## 为什么不能原地修改

如果直接修改共享 registry，一轮执行可能先看到旧 tool schema，后一步却调用到新实现；MCP 连接也可能
在工具调用尚未完成时被关闭。快照让“这一轮看到了什么”成为可回答、可测试的事实。

## 生效边界

- 插件和 Workspace MCP 的变化通常从下一轮生效；
- 被动 turn、Proactive tick 和 Drift run 都持有 lease；
- old generation terminate 按依赖逆序、幂等执行；
- tool search 解锁结果也属于本轮上下文，不跨 generation 偷渡。

## 失败边界

- 声明非法或某个 MCP 连不上：候选整体作废；
- watcher 之后发现输入修复：重新构建并自动恢复；
- 旧资源关闭失败：记录错误，继续关闭其余资源；
- lease 泄漏：会阻止旧 generation 回收，应通过 generation 状态和 reload journal 排查。

操作方法见[插件手册](../handbook/plugins.md)和[Workspace MCP 手册](../handbook/workspace-mcp.md)。
