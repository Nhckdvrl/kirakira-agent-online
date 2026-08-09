# 插件架构

插件系统把扩展能力编译为一个有代际的 runtime snapshot。全局清单只决定启停，插件代码自己声明
能力，运行时负责校验、排序、装配、换代和回收。

## 主要组件

| 组件 | 职责 |
| --- | --- |
| `PluginManager` | 发现、导入、初始化、终止和错误隔离 |
| `PluginRegistry` | 收集 phase、tool、hook、Channel、source 等声明 |
| `PluginGeneration` | 表示一次完整加载结果 |
| `RuntimeSnapshot` | 固定某一轮可见的插件、工具和 MCP 能力 |
| watcher / reload journal | 发现变更、发布新代际并记录失败 |

## 加载流程

```text
扫描 .kirakira/plugins/*/plugin.py
  → 读取 manifest 启用状态
  → 导入 Plugin 子类
  → initialize 并收集声明
  → 校验路径、名称和依赖
  → 编译 phase / tool / MCP / source
  → 原子发布新 generation
```

单个插件失败会撤销它已经注册的能力，并记入 errors；其他插件仍可发布。manifest 本身非法时整体失败，
因为启用集合无法可信确定。

## 扩展面

- 七个 lifecycle phase；
- tools 和 tool pre-hook；
- skills；
- MCP server；
- Channel；
- Proactive source、模块和排序 slot；
- 配置 schema、data dir 与 KV store。

Phase 和 Proactive 模块用 dependency DAG 排序。声明缺失依赖的模块及其下游会被禁用；循环依赖会记录
清晰错误。旧 phase 模块未全部声明 slot 时保持原顺序，见[决策 0001](../decisions/0001-plugin-slot-ordering-opt-in.md)。

## 热重载

安装、启用、停用、卸载或文件变化会构建候选 generation。候选完全成功后才原子替换当前 generation；
失败时旧 generation 继续服务。

在途 turn 持有 snapshot lease，所以不会在一半执行中丢失工具、hook 或 MCP 连接。旧代际等最后一个租约
释放后逆序 terminate。详细租约模型见[快照、代际与租约](./snapshot-leases.md)。

## 信任边界

插件是进程内 Python 代码，不是沙箱。安装后会在换代时导入和执行，只应使用可信来源。路径校验可以
阻止声明越出插件根，但不能限制插件代码自身能做什么。

密钥放在 `config.local.toml` 或外部环境，不应进入 manifest、日志、tool result 或对话历史。

## 操作入口

插件管理工具、最小示例和故障排查见[插件手册](../handbook/plugins.md)。
