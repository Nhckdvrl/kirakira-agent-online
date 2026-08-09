"""Akasha RAR(Ripple Activation & Recall)记忆引擎。

照 Reference `plugins/akasha` 移植的**第二套记忆引擎**。它与默认引擎是两条不同的路线:

- `coremem` 的 DefaultMemoryEngine:向量 lane + 关键词 lane → RRF 融合 → 注入预算;
- akasha:把记忆建成一张**激活图**,查询在图上做涟漪扩散(ripple activation),
  命中不只看与 query 的相似度,还看它被邻居激活的程度,并随时间衰减。

移植边界:
- `core.py` 与 `fast/` 逐字节镜像(它们零框架依赖,只用 numpy + jieba + stdlib);
- `store/graph_snapshot/replay/engine/config/memory_plugin` 只改 import 命名空间;
- 框架设施由 `agent/plugins/config.py` 与 `infra/persistence/json_store.py` 提供。

未移植的两个文件及理由见 docs/ALIGNMENT.md:`plugin.py`(依赖 PhaseFrame 与
MobileUiContribution,两者 kirakira 都没有)、`dashboard.py`(FastAPI,kirakira 用零依赖仪表盘)。
"""

from plugins.akasha.memory_plugin import MemoryPlugin

__all__ = ["MemoryPlugin"]
