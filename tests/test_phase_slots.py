"""Phase slot 依赖图契约(照 Reference agent/lifecycle/phase.py)。

现有 lifecycle 是"注册顺序即执行顺序",模块之间无法表达先后依赖。
本契约锁定 slot 语义:拓扑排序、级联禁用、循环依赖 fail loud、稳定排序。
"""

from __future__ import annotations

import unittest

from agent.lifecycle.phase import (
    inspect_phase,
    is_builtin_slot,
    topo_sort_modules,
)


class _Module:
    def __init__(self, slot: str, requires: tuple[str, ...] = ()) -> None:
        self.slot = slot
        self.requires = requires

    def __repr__(self) -> str:  # 便于断言失败时看清是谁
        return "<%s>" % self.slot


def _slots(modules) -> list[str]:
    return [getattr(module, "slot", type(module).__name__) for module in modules]


class TopoSortTests(unittest.TestCase):
    def test_dependency_order_is_respected(self) -> None:
        a = _Module("plug.a")
        b = _Module("plug.b", requires=("plug.a",))
        c = _Module("plug.c", requires=("plug.b",))
        # 故意乱序注册
        order = _slots(topo_sort_modules([c, b, a]))
        self.assertLess(order.index("plug.a"), order.index("plug.b"))
        self.assertLess(order.index("plug.b"), order.index("plug.c"))

    def test_independent_modules_keep_registration_order(self) -> None:
        first = _Module("plug.first")
        second = _Module("plug.second")
        self.assertEqual(
            _slots(topo_sort_modules([first, second])), ["plug.first", "plug.second"]
        )

    def test_plugin_modules_run_before_builtin_when_both_ready(self) -> None:
        builtin = _Module("before_turn.core")
        plugin = _Module("plug.custom")
        # 注册顺序把内置放前面,排序仍让插件先跑
        self.assertEqual(
            _slots(topo_sort_modules([builtin, plugin])),
            ["plug.custom", "before_turn.core"],
        )

    def test_missing_slot_declaration_fails_loud(self) -> None:
        class _NoSlot:
            pass

        with self.assertRaises(RuntimeError) as ctx:
            topo_sort_modules([_NoSlot()])
        self.assertIn("slot", str(ctx.exception))

    def test_duplicate_slot_fails_loud(self) -> None:
        with self.assertRaises(RuntimeError) as ctx:
            topo_sort_modules([_Module("plug.dup"), _Module("plug.dup")])
        self.assertIn("重复", str(ctx.exception))

    def test_cycle_fails_loud(self) -> None:
        a = _Module("plug.a", requires=("plug.b",))
        b = _Module("plug.b", requires=("plug.a",))
        with self.assertRaises(RuntimeError) as ctx:
            topo_sort_modules([a, b])
        self.assertIn("循环依赖", str(ctx.exception))


class CascadingDisableTests(unittest.TestCase):
    def test_module_with_missing_dependency_is_disabled(self) -> None:
        good = _Module("plug.good")
        orphan = _Module("plug.orphan", requires=("plug.nonexistent",))
        self.assertEqual(_slots(topo_sort_modules([good, orphan])), ["plug.good"])

    def test_disable_cascades_to_dependents(self) -> None:
        # b 依赖不存在的 slot 被禁用 → 依赖 b 的 c 也必须被禁用
        b = _Module("plug.b", requires=("plug.missing",))
        c = _Module("plug.c", requires=("plug.b",))
        keep = _Module("plug.keep")
        self.assertEqual(_slots(topo_sort_modules([b, c, keep])), ["plug.keep"])

    def test_builtin_slots_are_not_disabled(self) -> None:
        # 内置模块缺依赖是配置问题,不该被静默摘掉
        builtin = _Module("before_turn.core", requires=("plug.missing",))
        self.assertEqual(_slots(topo_sort_modules([builtin])), ["before_turn.core"])

    def test_capability_style_requires_do_not_disable(self) -> None:
        # 带冒号的是能力引用,不是模块 slot,不参与禁用判断
        module = _Module("plug.a", requires=("mcp:some-server",))
        self.assertEqual(_slots(topo_sort_modules([module])), ["plug.a"])


class HelperTests(unittest.TestCase):
    def test_is_builtin_slot(self) -> None:
        self.assertTrue(is_builtin_slot("after_turn.persist"))
        self.assertFalse(is_builtin_slot("plug.custom"))

    def test_inspect_phase_renders_order_and_edges(self) -> None:
        a = _Module("plug.a")
        b = _Module("plug.b", requires=("plug.a",))
        text = inspect_phase([b, a])
        self.assertIn("plug.a", text)
        self.assertIn("plug.a -> plug.b", text)


class ManagerIntegrationTests(unittest.TestCase):
    """PluginManager._collect 的 slot 排序:全员声明才启用,否则保持原顺序。"""

    def _order(self, modules):
        from agent.plugins import PluginManager

        return _slots(PluginManager._order_phase_modules("before_turn", modules))

    def test_all_slotted_modules_get_topo_sorted(self) -> None:
        a = _Module("plug.a")
        b = _Module("plug.b", requires=("plug.a",))
        self.assertEqual(self._order([b, a]), ["plug.a", "plug.b"])

    def test_mixed_modules_keep_registration_order(self) -> None:
        class _Plain:
            pass

        plain = _Plain()
        slotted = _Module("plug.b", requires=("plug.a",))
        # 混用时不重排:避免未声明 slot 的老模块被隐式挪位置
        result = self._order([slotted, plain])
        self.assertEqual(result[0], "plug.b")

    def test_bad_declaration_keeps_order_instead_of_crashing_phase(self) -> None:
        a = _Module("plug.a", requires=("plug.b",))
        b = _Module("plug.b", requires=("plug.a",))
        # 成环是插件的声明错误,不能把整个相位打挂
        self.assertEqual(self._order([a, b]), ["plug.a", "plug.b"])


if __name__ == "__main__":
    unittest.main()
