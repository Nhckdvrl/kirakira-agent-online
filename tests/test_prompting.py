"""Reference-compatible prompt assembly and skill activation contracts."""

from datetime import datetime
from pathlib import Path
import tempfile
import unittest

from agent.prompting.context_builder import ContextBuilder
from core.memory.legacy import MemoryRuntime
from agent.prompting import SYSTEM_CONTEXT_FRAME_MARKER
from session.manager import SessionManager
from agent.skills import SkillLoader


class PromptingTests(unittest.TestCase):
    def test_dynamic_context_frame_sits_between_history_and_current_user(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            memory = MemoryRuntime(root, SessionManager(root))
            builder = ContextBuilder(root, memory)

            result = builder.render(
                channel="cli",
                chat_id="chat",
                content="current",
                timestamp=datetime.now().astimezone(),
                history=[{"role": "user", "content": "old"}],
                retrieved_memory_block="remembered fact",
            )

            self.assertEqual(result.messages[1]["content"], "old")
            self.assertTrue(result.messages[-2]["content"].startswith(SYSTEM_CONTEXT_FRAME_MARKER))
            self.assertIn("remembered fact", result.messages[-2]["content"])
            self.assertNotIn("remembered fact", result.system_prompt)
            self.assertIn("current", result.messages[-1]["content"])

    def test_static_sections_are_cached_and_semantic_trim_is_named(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            skill_dir = root / "skills" / "demo"
            skill_dir.mkdir(parents=True)
            (skill_dir / "SKILL.md").write_text(
                "---\nname: demo\ndescription: demo skill\n---\nbody\n"
            )
            memory = MemoryRuntime(root, SessionManager(root))
            builder = ContextBuilder(root, memory)
            kwargs = dict(
                channel="cli",
                chat_id="chat",
                content="current",
                timestamp=datetime.now().astimezone(),
                history=[],
            )

            builder.render(**kwargs)
            second = builder.render(**kwargs)
            cached = {item.name: item.cache_hit for item in second.debug_breakdown}
            self.assertTrue(cached["identity"])
            self.assertTrue(cached["behavior_rules"])
            self.assertTrue(cached["skills_catalog"])

            trimmed = builder.render(**kwargs, disabled_sections={"skills_catalog"})
            self.assertNotIn("skills_catalog", {item.name for item in trimmed.debug_breakdown})
            self.assertNotIn("demo skill", trimmed.system_prompt)

    def test_always_skill_is_loaded_without_an_explicit_mention(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            skill_dir = root / "skills" / "always-demo"
            skill_dir.mkdir(parents=True)
            (skill_dir / "SKILL.md").write_text(
                "---\nname: always-demo\ndescription: always\nalways: true\n---\nALWAYS BODY\n"
            )
            loader = SkillLoader(root / "skills")
            self.assertEqual(loader.always_names(), ["always-demo"])

            memory = MemoryRuntime(root, SessionManager(root))
            result = ContextBuilder(root, memory).render(
                channel="cli",
                chat_id="chat",
                content="hello",
                timestamp=datetime.now().astimezone(),
                history=[],
            )
            self.assertIn("ALWAYS BODY", result.context_frame)


if __name__ == "__main__":
    unittest.main()
