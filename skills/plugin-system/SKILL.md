---
name: plugin-system
description: 说明并执行 kirakira 插件的安装、启用、禁用、卸载、诊断，以及插件自带的 skill/MCP/工具/生命周期与 manifest 管理。当用户询问或要求处理 kirakira 插件、插件自带 MCP、skill、插件配置、安装、更新、启用、禁用、卸载或排障时使用。独立本地 MCP server 改用 manage-workspace-mcp。
when_to_use: 用户要求安装/启停/卸载/诊断 kirakira 插件，或询问插件自带能力（skill、MCP、工具、生命周期）时。
metadata: {"kirakira": {"always": false}}
---

# kirakira 插件系统

优先直接完成明确的插件管理请求，并在修改后验证。用 `tool_search` 解锁插件工具（都是 deferred）。

独立 binary、脚本或本地项目需要作为 MCP 常驻时，加载 `manage-workspace-mcp`；不要为它创建插件。

## 数据位置与生效方式

- 插件安装到 **workspace 内**：`<workspace>/.kirakira/plugins/<name>/`，根目录必须有 `plugin.py`。
- 启停清单：`<workspace>/.kirakira/manifest.toml`，格式 `[plugins.<name>] enabled = true|false`；未记录默认启用。
- 插件数据/配置：`<workspace>/.kirakira/plugin-data/<name>/`（`kv.json`、`config.local.toml` 等）。
- 安装、启用、禁用、卸载都会触发热重载，不要求重启 runtime。
- 不要创建旧式 `plugin.json`、`manifest.yaml`、`registry.json` 或 `mcp/servers.json`。

## 能力声明

插件全部用代码在 `plugin.py` 里声明 `Plugin` 子类，含 `name`、`version`。可声明：
`skill_roots()`、`mcp_servers()`、`register_tools()`、`tool_hooks()`、各 `*_modules()` phase 钩子、
`channels()`、`ConfigModel`。公共 runtime 不应出现具体插件名或业务路径特判。

## 工具

| 工具 | 作用 |
|---|---|
| `plugin_list` | 列出已加载插件与加载错误 |
| `plugin_doctor` | 校验插件结构/skill/MCP 声明，不执行代码 |
| `plugin_install` | 从本地目录或 HTTPS Git 仓库安装（需含根 `plugin.py`） |
| `plugin_enable` / `plugin_disable` | 改写 manifest 启停状态 |
| `plugin_uninstall` | 删除插件目录与 manifest 条目，**保留** plugin-data |

## 安装

```text
plugin_install(source="/path/to/plugin_repo")   # 本地目录
plugin_install(source="https://github.com/user/plugin.git")   # HTTPS Git
```

安装成功后等待热重载完成，再用 `plugin_list` 确认已加载、`plugin_doctor <name>` 校验结构。

Git 安装只取仓库已提交的 HEAD，不复制未提交文件；需要用最新代码时先提交（远程源还要先 push）。

## 启用 / 禁用 / 卸载

```text
plugin_disable(name="demo")
plugin_enable(name="demo")
plugin_uninstall(name="demo")
```

三者会修改磁盘状态并触发热重载。卸载保留 `plugin-data`，不要手动删数据、配置、token 或数据库。

## 完成判定

工具返回成功后仍需确认新代际已经发布。汇报完成前必须：

```text
┌─ plugin_list 显示目标插件已加载（或已消失），errors 为空
├─ plugin_doctor 对目标插件结构 ok
└─ 发起一次真实请求，确认目标 skill/MCP/工具确实可用
```

`plugin_doctor` 只证明结构可加载，不证明具体 skill 已生效，也不能替代真实行为验证。

## 配置

读取插件 `plugin.py` 的 `ConfigModel`，再编辑 `<workspace>/.kirakira/plugin-data/<name>/config.local.toml`。
不要把插件配置写回主 `config.toml`。

## 能力排查

```text
┌─ skills → 检查 skill_roots()，或插件根下的 skills/
├─ MCP    → 检查 mcp_servers()、command、cwd
├─ 工具    → 检查 register_tools() 与 @tool 装饰器
└─ 生命周期 → 检查 initialize()/terminate() 与加载日志（plugin_list 的 errors）
```
