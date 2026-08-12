# 插件架构

在线插件系统把每个用户声明的远程能力编译为一轮固定的 runtime snapshot。共享 worker 不导入用户
Python；插件服务通过公共 HTTPS 协议提供能力，运行时负责校验、排序、装配、租约和错误隔离。

## 主要组件

| 组件 | 职责 |
| --- | --- |
| `CloudPluginStore` | durable manifest、密文凭据、job/source 状态 |
| `CloudPluginService` | 校验、远程调用、排序和任务执行 |
| `RuntimeSnapshot` | 固定某一轮可见的 phase、tool、hook、MCP 与 Skill |
| snapshot lease | 保证在途 turn 的能力集合不发生变化 |

## 加载流程

```text
读取当前 user 的 enabled manifest
  → 校验 identity、HTTPS endpoint、DNS/allowlist 和依赖
  → 解密请求头，仅在出站请求中使用
  → 编译 phase / tool / hook / MCP / source
  → 绑定本轮 snapshot lease
```

单个远程调用失败只影响该能力并形成结构化错误，不会执行服务端任意代码。manifest 的 identity 字段不能
被远程 patch 改写。

## 扩展面

- 七个 lifecycle phase；
- tools 和 tool pre-hook；
- manifest-contributed MCP server；
- Proactive source 与 durable plugin job；
- 一个 job 内最多 5 次受控 Cloud LLM 请求及 `/complete` 回调。

Phase 和 Proactive 模块用 dependency DAG 排序。声明缺失依赖的模块及其下游会被禁用；循环依赖会记录
清晰错误。旧 phase 模块未全部声明 slot 时保持原顺序，见[决策 0001](../decisions/0001-plugin-slot-ordering-opt-in.md)。

## 快照与更新

安装、启用、停用或删除只影响后续 turn。在途 turn 持有 snapshot lease，所以不会在一半执行中丢失
工具、hook 或 MCP 连接。详细租约模型见[快照、代际与租约](./snapshot-leases.md)。

## 信任边界

远程插件仍能看到显式发送给它的参数，因此用户只应安装可信服务。endpoint 禁止私网、loopback、userinfo
和非 HTTPS 地址，并可再用域名 allowlist 收紧。认证请求头使用服务端 Fernet key 加密，不进入 manifest、
日志、tool result 或对话历史。

## 操作入口

浏览器设置页和 `/v1/plugins` 提供创建、查看与删除入口。旧本地插件代码只保留为原算法的回归夹具，
不属于发行产品入口。
