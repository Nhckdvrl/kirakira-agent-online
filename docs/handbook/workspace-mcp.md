# 配置 Workspace MCP

非插件 MCP server 写在 `<workspace>/mcp/servers/*.toml`。一个文件对应一个 server，文件名必须与
`name` 相同。运行时按内容 revision 热重载，不再使用旧的 `mcp_servers.json`。

```text
<workspace>/mcp/
├── servers/fitbit.toml
└── fitbit-mcp/
    └── run_mcp.py
```

```toml
schema_version = 1
name = "fitbit"
command = ["python", "run_mcp.py"]
cwd = "../fitbit-mcp"
watch_paths = ["../fitbit-mcp/run_mcp.py"]

[env]
LOG_LEVEL = "INFO"
```

## 路径和校验

`cwd` 与 `watch_paths` 相对声明文件解析，最终必须位于 `<workspace>/mcp/` 内。声明只允许
`schema_version`、`name`、`command`、`cwd`、`env`、`watch_paths`；未知字段和错误版本会直接拒绝。

revision 由文件内容和 watch path 内容计算，不依赖 mtime。只 `touch` 文件不会触发换代。

## Agent 管理工具

| 工具 | 作用 |
| --- | --- |
| `workspace_mcp_apply` | 新建或更新声明，并立即校验、发布 |
| `workspace_mcp_remove` | 删除声明并发布新代际 |
| `workspace_mcp_status` | 查看声明、代际和最近错误 |

这些 deferred tools 需要先用 `tool_search` 解锁。`apply` 失败会回滚声明，并把旧内容备份到
`mcp/backups/`。`status` 只显示 env 键名，不回显密钥值。新工具从下一轮开始可见。

## 换代语义

- 所有声明作为一批候选发布；任一声明或连接失败，整批作废；
- 失败时旧代际继续服务，修好文件后 watcher 会自动重试；
- 删除全部声明会发布一个合法的空代际；
- 在途 turn 继续使用自己锁定的旧代际，租约释放后才关闭旧连接。

完整原理见[快照、代际与租约](../architecture/snapshot-leases.md)。

## 排查

REPL 的 `/tools` 显示当前代际的 MCP 工具，名称形如 `mcp_<server>__<tool>`。看不到工具时先检查
`workspace_mcp_status` 的声明和 `lastError`。MCP 工具默认 deferred，不会一直占用 prompt。
