# 文档索引

Kirakira 的正式产品形态是多用户 Cloud Agent。文档只保留四个主要入口：

| 文档 | 内容 |
| --- | --- |
| [Cloud 总方案与实施记录](./cloud-engineering/plan.md) | 产品目标、Web 调研、原始完整方案、评审、实施结果和仍需确认项 |
| [架构总览](./architecture/overview.md) | 在线请求、worker、PostgreSQL、sandbox、Memory、Proactive/Drift 的边界 |
| [当前状态](./status/current.md) | 已完成能力与发布环境外部条件 |
| [验证记录](./operations/verification.md) | 自动化、真实 PostgreSQL、pgvector 与构建证据 |

算法专项说明：

- [Memory 架构](./architecture/memory.md)
- [Proactive 与 Drift](./architecture/proactive.md)
- [插件架构](./architecture/plugins.md)
- [快照与租约](./architecture/snapshot-leases.md)

`handbook/`、旧控制面和本地 workspace 文档属于算法回归实现的历史资料，不是 Cloud 产品启动说明。
公开部署和 API 使用以根目录 README 与上述四个入口为准。
