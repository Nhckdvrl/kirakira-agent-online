"""Phase 模块的 slot 依赖图(照 Reference `agent/lifecycle/phase.py`)。

kirakira 现有的 lifecycle 是 7 个相位钩子 + 插件按注册顺序 append 进列表——顺序即
注册顺序,模块之间无法表达"我要在谁之后跑"。这在插件多起来之后必然出问题:两个插件
都想改同一段上下文时,谁先谁后取决于加载顺序这种偶然因素。

本模块补上 Reference 的 slot 语义:
- 每个模块声明 `slot`(唯一标识)与 `requires`(依赖哪些 slot);
- 拓扑排序决定执行顺序,循环依赖直接 fail loud;
- 依赖的 slot 不存在时**级联禁用**该模块,而不是让它带着坏假设跑——
  这与本仓既有的"能力以运行时为准""宁可全旧不要半新"是同一取向。

Reference 的 PhaseFrame(模块间用 frame.slots 传中间产物)暂未移植:kirakira 的相位模块
目前还是 ctx 对象签名,等模块签名迁移时再一起引入,不先摆一个没人用的结构。

内置 slot(`before_turn.` 等前缀)不参与禁用:它们是 runtime 自带的,缺依赖说明是
配置问题,不该被静默摘掉。
"""

from __future__ import annotations

import logging
from typing import Dict, List, Mapping, Protocol, Sequence, cast

logger = logging.getLogger(__name__)

_BUILTIN_SLOT_PREFIXES = (
    "before_turn.",
    "before_reasoning.",
    "prompt_render.",
    "before_step.",
    "after_step.",
    "after_reasoning.",
    "after_turn.",
)


class SlotModule(Protocol):
    slot: str


def is_builtin_slot(slot: str) -> bool:
    return slot.startswith(_BUILTIN_SLOT_PREFIXES)


def _is_module_slot(slot: str) -> bool:
    """模块 slot 形如 `a.b`;带冒号的是能力/资源引用,不参与模块禁用判断。"""
    return "." in slot and ":" not in slot


def _module_requires(
    module: object,
    known_slots: Mapping[str, object],
) -> tuple[str, ...]:
    requires = tuple(str(slot) for slot in getattr(module, "requires", ()))
    return tuple(slot for slot in requires if slot in known_slots)


def _missing_module_requires(module: object, active_slots: set[str]) -> tuple[str, ...]:
    return tuple(
        req
        for req in (str(slot) for slot in getattr(module, "requires", ()))
        if _is_module_slot(req) and req not in active_slots
    )


def _active_module_slots(slot_map: Mapping[str, object]) -> set[str]:
    """级联禁用:依赖缺失的模块被摘掉后,依赖它的模块也随之被摘掉。"""
    active = set(slot_map)
    while True:
        disabled: set[str] = set()
        for slot, module in slot_map.items():
            if slot not in active or is_builtin_slot(slot):
                continue
            missing = _missing_module_requires(module, active)
            if missing:
                logger.warning(
                    "Phase 模块依赖不存在，已禁用模块: module=%s requires=%s",
                    slot,
                    ", ".join(missing),
                )
                disabled.add(slot)
        if not disabled:
            return active
        active -= disabled


def topo_sort_modules(modules: Sequence[object]) -> List[object]:
    """按 slot 依赖拓扑排序。缺 slot 声明 / slot 重复 / 循环依赖都 fail loud。"""
    slot_map: Dict[str, SlotModule] = {}
    slot_order: Dict[str, int] = {}
    for index, module in enumerate(modules):
        slot = getattr(module, "slot", None)
        if not isinstance(slot, str) or not slot:
            raise RuntimeError("模块缺少 slot 声明: %s" % type(module).__name__)
        if slot in slot_map:
            raise RuntimeError("模块 slot 重复: %s" % slot)
        slot_map[slot] = cast(SlotModule, module)
        slot_order[slot] = index

    active_slots = _active_module_slots(slot_map)
    slot_map = {slot: module for slot, module in slot_map.items() if slot in active_slots}

    in_degree = {slot: 0 for slot in slot_map}
    dependents: Dict[str, List[str]] = {slot: [] for slot in slot_map}
    for slot, module in slot_map.items():
        for req in _module_requires(module, slot_map):
            in_degree[slot] += 1
            dependents[req].append(slot)

    queue = [slot for slot, degree in in_degree.items() if degree == 0]
    sorted_modules: List[object] = []
    while queue:
        # 同时就绪时:插件模块先于内置模块,其次按注册顺序——保证结果稳定可预测。
        queue.sort(key=lambda item: (is_builtin_slot(item), slot_order[item]))
        slot = queue.pop(0)
        sorted_modules.append(slot_map[slot])
        for dependent in dependents[slot]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(sorted_modules) != len(slot_map):
        unresolved = sorted(slot for slot, degree in in_degree.items() if degree > 0)
        raise RuntimeError("模块循环依赖: %s" % ", ".join(unresolved))
    return sorted_modules


def inspect_phase(modules: Sequence[object]) -> str:
    """把一个相位的执行顺序与依赖关系渲染成可读文本,便于排查插件顺序问题。"""
    sorted_modules = cast(List[SlotModule], topo_sort_modules(modules))
    chain = "\n".join(
        "  %2d. %s%s"
        % (
            index,
            "[B] " if is_builtin_slot(module.slot) else "[P] ",
            module.slot,
        )
        for index, module in enumerate(sorted_modules)
    )
    slot_map = {module.slot: module for module in sorted_modules}
    edges: List[str] = []
    for module in sorted_modules:
        for req in _module_requires(module, slot_map):
            edges.append("  %s -> %s" % (req, module.slot))
    dependencies = "\n".join(edges) if edges else "  (无依赖)"
    return "执行顺序:\n%s\n\n依赖:\n%s" % (chain, dependencies)
