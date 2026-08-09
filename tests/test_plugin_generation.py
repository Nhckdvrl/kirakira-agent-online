"""per-plugin 代际与租约契约(照 Reference agent/plugins/generation.py + snapshot.py)。

核心语义:
- 语义检查决定候选代际能否准入;未通过不得发布,旧代际继续服务;
- 换代后旧代际转 retired,但只有租约归零才 quiesce(在途 turn 不被抽走能力);
- revision 由源码 + 配置内容决定,内容不变则代际 id 稳定。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from agent.plugins.generation import (
    GateResult,
    PluginContributions,
    PluginGeneration,
    PluginGenerationRegistry,
    compute_generation_id,
    file_revision,
)
from agent.plugins.specs import PluginSemanticCheck


def _generation(
    plugin_id: str = "demo",
    *,
    source_revision: str = "src1",
    config_revision: str = "cfg1",
    checks: tuple[PluginSemanticCheck, ...] = (),
) -> PluginGeneration:
    gate = GateResult.from_checks(
        plugin_id=plugin_id,
        candidate_revision=f"{source_revision}:{config_revision}",
        checks=checks,
    )
    return PluginGeneration(
        plugin_id=plugin_id,
        generation_id=compute_generation_id(
            plugin_id=plugin_id,
            source_revision=source_revision,
            config_revision=config_revision,
        ),
        module_path="demo.plugin",
        source_revision=source_revision,
        config_revision=config_revision,
        instance=object(),
        contributions=PluginContributions(),
        gate_result=gate,
    )


class GateTests(unittest.TestCase):
    def test_all_passing_checks_yield_passed_gate(self) -> None:
        gate = GateResult.from_checks(
            plugin_id="demo",
            candidate_revision="r",
            checks=(PluginSemanticCheck.ok("a"), PluginSemanticCheck.ok("b")),
        )
        self.assertTrue(gate.passed)
        self.assertEqual(gate.failure_reason, "")

    def test_any_failing_check_fails_gate_with_reason(self) -> None:
        gate = GateResult.from_checks(
            plugin_id="demo",
            candidate_revision="r",
            checks=(
                PluginSemanticCheck.ok("a"),
                PluginSemanticCheck.fail("needs_mcp", "server missing"),
            ),
        )
        self.assertFalse(gate.passed)
        self.assertIn("needs_mcp", gate.failure_reason)
        self.assertIn("server missing", gate.failure_reason)

    def test_no_checks_is_passing(self) -> None:
        self.assertTrue(
            GateResult.from_checks(
                plugin_id="demo", candidate_revision="r", checks=()
            ).passed
        )


class GenerationRegistryTests(unittest.TestCase):
    def test_publish_sets_active_and_retires_previous(self) -> None:
        registry = PluginGenerationRegistry()
        first = _generation(source_revision="v1")
        self.assertIsNone(registry.publish(first))
        self.assertEqual(first.state, "active")

        second = _generation(source_revision="v2")
        previous = registry.publish(second)
        self.assertIs(previous, first)
        self.assertEqual(first.state, "retired")
        self.assertEqual(second.state, "active")
        self.assertIs(registry.current("demo"), second)

    def test_failed_gate_cannot_be_published(self) -> None:
        registry = PluginGenerationRegistry()
        bad = _generation(checks=(PluginSemanticCheck.fail("x", "boom"),))
        with self.assertRaises(ValueError):
            registry.publish(bad)
        self.assertIsNone(registry.current("demo"))

    def test_retired_generation_with_lease_does_not_quiesce(self) -> None:
        registry = PluginGenerationRegistry()
        old = _generation(source_revision="v1")
        registry.publish(old)
        # 在途 turn 持有旧代际租约
        old.acquire()
        registry.publish(_generation(source_revision="v2"))
        self.assertEqual(old.state, "retired")
        self.assertFalse(old.can_quiesce)
        self.assertEqual(registry.drain_quiescible(), ())

        # 租约释放后才可 quiesce
        old.release()
        self.assertTrue(old.can_quiesce)
        drained = registry.drain_quiescible()
        self.assertEqual(len(drained), 1)
        self.assertEqual(drained[0].state, "quiesced")
        self.assertEqual(registry.drain_quiescible(), ())

    def test_retired_generation_rejects_new_leases(self) -> None:
        registry = PluginGenerationRegistry()
        old = _generation(source_revision="v1")
        registry.publish(old)
        registry.publish(_generation(source_revision="v2"))
        with self.assertRaises(RuntimeError):
            old.acquire()

    def test_double_release_is_loud(self) -> None:
        generation = _generation()
        generation.acquire()
        generation.release()
        with self.assertRaises(RuntimeError):
            generation.release()

    def test_retire_all_moves_everything_out_of_active(self) -> None:
        registry = PluginGenerationRegistry()
        registry.publish(_generation("a"))
        registry.publish(_generation("b"))
        self.assertEqual(len(registry.active), 2)
        registry.retire_all()
        self.assertEqual(registry.active, ())
        self.assertEqual(len(registry.retired), 2)


class RevisionTests(unittest.TestCase):
    def test_file_revision_tracks_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plugin.py"
            path.write_text("a = 1", encoding="utf-8")
            first = file_revision(path)
            self.assertTrue(first)
            self.assertEqual(first, file_revision(path))
            path.write_text("a = 2", encoding="utf-8")
            self.assertNotEqual(first, file_revision(path))

    def test_missing_file_revision_is_empty(self) -> None:
        self.assertEqual(file_revision(Path("/definitely/missing/plugin.py")), "")

    def test_generation_id_stable_for_same_revisions(self) -> None:
        a = compute_generation_id(plugin_id="p", source_revision="s", config_revision="c")
        b = compute_generation_id(plugin_id="p", source_revision="s", config_revision="c")
        different = compute_generation_id(
            plugin_id="p", source_revision="s2", config_revision="c"
        )
        self.assertEqual(a, b)
        self.assertNotEqual(a, different)


if __name__ == "__main__":
    unittest.main()
