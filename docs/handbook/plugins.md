# 插件开发与管理

插件用代码声明能力。每个插件根目录必须有 `plugin.py`，其中定义 `Plugin` 子类；旧版
`.aka-plugin/plugin.json` 已不再使用。

```text
<workspace>/.kirakira/
├── manifest.toml          # 只记录启用状态
├── plugins/<plugin_id>/   # 插件代码
│   └── plugin.py
└── plugin-data/<plugin_id>/ # 插件持久数据
```

## 最小插件

```python
from agent.plugins import Plugin


class DemoPlugin(Plugin):
    name = "demo"
    version = "1.0.0"
    desc = "演示插件"
```

插件还可以声明：

- lifecycle phase 模块；
- `@tool` 工具和 `@on_tool_pre` 工具拦截器；
- skills 目录；
- MCP server；
- Channel；
- Proactive source 和模块。

详细扩展面见[插件架构](../architecture/plugins.md)。

## 管理工具

| 工具 | 作用 | 生效时间 |
| --- | --- | --- |
| `plugin_install` | 从本地目录或 HTTPS Git 地址安装 | 热重载后生效 |
| `plugin_enable` | 启用已安装插件 | 热重载后生效 |
| `plugin_disable` | 停用插件 | 热重载后生效 |
| `plugin_uninstall` | 卸载代码，保留 plugin-data | 热重载后生效 |
| `plugin_list` | 查看插件、版本、能力和加载错误 | 立即 |
| `plugin_doctor` | 检查目录结构与能力声明 | 立即 |

这些操作不要求重启 runtime。安装来源中的代码会在随后的插件换代中加载，因此只应安装可信代码。
正在运行的 turn 持有旧快照，换代不会抽走它已经看到的工具或 MCP 连接。

## 启停清单

`<workspace>/.kirakira/manifest.toml` 只记录启用状态：

```toml
[plugins."demo"]
enabled = true
```

能力、路径和配置 schema 仍由 `plugin.py` 声明。清单中没有记录的插件默认启用。清单结构非法时
整体加载失败，避免把错误配置误解成“全部启用”。

## Skills 与 MCP

```python
from agent.plugins import McpServerSpec, Plugin


class DemoPlugin(Plugin):
    name = "demo"

    @classmethod
    def skill_roots(cls) -> tuple[str, ...]:
        return ("skills",)

    @classmethod
    def mcp_servers(cls) -> list[McpServerSpec]:
        return [
            McpServerSpec(
                name="demo-mcp",
                command=("python", "./server.py"),
                cwd=".",
            )
        ]
```

相对路径按插件根解析，不能越出插件目录。运行 MCP 时会注入
`KIRAKIRA_PLUGIN_DATA_DIR`。插件 MCP 与 Workspace MCP 使用同一套代际换代和租约语义，server
名称不能冲突。

## 配置与数据

- `config.toml`：插件默认配置；
- `config.local.toml`：本机覆盖，适合密钥和私有地址；
- `self.context.data_dir`：插件持久数据目录；
- `self.context.kv_store`：原子 JSON KV。

不要把运行状态写进插件代码目录。

## Lifecycle 注意事项

七个 phase 是 `before_turn`、`before_reasoning`、`prompt_render`、`before_step`、`after_step`、
`after_reasoning`、`after_turn`。模块可以用 priority 和 slot dependency 排序。

`prompt_render` 每次 context retry 都会重新运行，因此必须幂等，不应执行外部副作用。需要观测
上下文时使用 `ContextPrepared`、`ContextBudgetUpdated` 等 observer 事件；observer 失败只记录日志，
不会阻断回复。

## 失败边界

- 单个插件导入或初始化失败：撤销该插件已注册的能力，其他插件继续加载；
- skill 路径或 MCP `cwd` 越界：该插件加载失败；
- manifest 非法：整体失败，因为启用集合不再可信；
- 新一代 MCP 连接失败：候选代际作废，旧代际继续服务；
- terminate 按加载逆序执行，并要求幂等。
