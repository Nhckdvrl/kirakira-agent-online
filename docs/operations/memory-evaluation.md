# 记忆评测

记忆评测分两层：免费、确定性的链路 Gate，以及需要真实模型的质量评测。两者不能混报。

## 确定性 Gate

离线 runner 在隔离 workspace 中对 DefaultMemoryEngine 和 Akasha v1 执行同一批样本，检查：

- 数据能摄入并持久化；
- query 能返回证据；
- source reference 可以回到原样本；
- 不同引擎和 workspace 不串数据；
- EM、F1、MRR 等基础指标计算稳定。

```bash
uv run pytest -q tests/test_memory_eval_offline.py
```

这层适合 CI，能拦住 wiring、持久化和证据链回归，但不能判断模型回答是否聪明。

## 在线链路 Smoke

```bash
uv run kirakira-verify-online
```

该验证使用真实 provider，检查 embedding、Default 工具链、上下文投影和 Akasha 证据消费。最新结果见
[验证记录](./verification.md)。

## 模型质量评测

完整 LongMemEval 还需要真实 AgentLoop 生成回答，并用官方口径或明确的 Judge 计算质量。运行时应把以下
信息分别报告：

- 数据集版本和样本范围；
- engine 与配置；
- retrieval 指标；
- answer Judge/F1/EM；
- token、请求数和费用；
- 失败、跳过和解析异常。

## 当前边界

尚未完成的质量项只在[当前状态](../status/current.md)维护，包括完整 LongMemEval、knowledge update 的
replacement oracle、Akasha 新版 parity 和 PersonaMem schema 漂移适配。离线 Gate 通过不等于这些质量项
已经通过。

相关实现位于 `eval/longmemeval/`，架构见[记忆架构](../architecture/memory.md)。
