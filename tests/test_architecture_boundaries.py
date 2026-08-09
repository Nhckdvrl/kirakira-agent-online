"""Guard canonical owners and the external-reference boundary."""

import ast
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOTS = (
    "agent",
    "bootstrap",
    "bus",
    "core",
    "eval",
    "frontend",
    "infra",
    "kirakira_agent",
    "memory2",
    "migrations",
    "plugin_packages",
    "plugins",
    "proactive_v2",
    "session",
    "utils",
)


def test_channel_sources_live_in_canonical_package() -> None:
    for name in (
        "base.py",
        "contract.py",
        "reply_context.py",
        "telegram_channel.py",
        "telegram_utils.py",
    ):
        assert (ROOT / "infra/channels" / name).is_file()
    assert not (ROOT / "kirakira_agent/channels").exists()


def test_upstream_pins_are_metadata_only() -> None:
    for path in (
        ROOT / "infra/channels/REFERENCE_PIN",
        ROOT / "agent/SUPERVISOR_REFERENCE_PIN",
    ):
        assert re.fullmatch(r"[0-9a-f]{40}", path.read_text(encoding="utf-8").strip())


def test_supervisor_uses_canonical_owner() -> None:
    from agent.supervisor import RESTART_EXIT_CODE

    assert RESTART_EXIT_CODE == 75


def test_production_code_does_not_read_reference_checkout() -> None:
    violations: list[str] = []
    for source_root in SOURCE_ROOTS:
        for path in (ROOT / source_root).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.BinOp)
                    and isinstance(node.op, ast.Div)
                    and isinstance(node.right, ast.Constant)
                    and node.right.value == "Reference"
                ):
                    violations.append(f"{path.relative_to(ROOT)}:{node.lineno}")
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "Path"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == "Reference"
                ):
                    violations.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert violations == []


def test_runtime_architecture_has_single_canonical_owner() -> None:
    legacy_paths = (
        "kirakira_agent/_compat",
        "kirakira_agent/akasha",
        "kirakira_agent/channels",
        "kirakira_agent/control",
        "kirakira_agent/coremem",
        "kirakira_agent/drift",
        "kirakira_agent/eval",
        "kirakira_agent/mcp",
        "kirakira_agent/models",
        "kirakira_agent/proactive",
        "kirakira_agent/prompting",
        "kirakira_agent/tools",
        "kirakira_agent/tui",
        "kirakira_agent/agent.py",
        "kirakira_agent/bootstrap.py",
        "kirakira_agent/bus.py",
        "kirakira_agent/cli.py",
        "kirakira_agent/config.py",
        "kirakira_agent/context.py",
        "kirakira_agent/dashboard.py",
        "kirakira_agent/embeddings.py",
        "kirakira_agent/entry.py",
        "kirakira_agent/event_bus.py",
        "kirakira_agent/events.py",
        "kirakira_agent/lifecycle.py",
        "kirakira_agent/memory.py",
        "kirakira_agent/memory_admin.py",
        "kirakira_agent/observe.py",
        "kirakira_agent/ports.py",
        "kirakira_agent/readiness.py",
        "kirakira_agent/retrieval.py",
        "kirakira_agent/runtime.py",
        "kirakira_agent/schema.py",
        "kirakira_agent/session.py",
        "kirakira_agent/subagent.py",
        "kirakira_agent/supervisor.py",
        "kirakira_agent/turns.py",
        "kirakira_agent/plugins.py",
        "kirakira_agent/snapshot.py",
        "kirakira_agent/tool_hooks.py",
    )
    assert [path for path in legacy_paths if (ROOT / path).exists()] == []


def test_brand_package_is_only_a_public_entry_shell() -> None:
    files = {
        path.name
        for path in (ROOT / "kirakira_agent").iterdir()
        if path.is_file() and path.suffix == ".py"
    }
    assert files == {"__init__.py", "__main__.py"}

    violations: list[str] = []
    for source_root in SOURCE_ROOTS:
        if source_root == "kirakira_agent":
            continue
        for path in (ROOT / source_root).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                else:
                    continue
                if any(name == "kirakira_agent" or name.startswith("kirakira_agent.") for name in names):
                    violations.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert violations == []


def test_required_architecture_owners_are_real_packages() -> None:
    required = (
        "agent/control",
        "agent/core",
        "agent/lifecycle",
        "agent/looping",
        "agent/mcp",
        "agent/migrations",
        "agent/model_runtime",
        "agent/plugins",
        "agent/prompting",
        "agent/retrieval",
        "agent/tool_hooks",
        "agent/tools",
        "agent/turns",
        "bootstrap",
        "bus",
        "core/memory",
        "core/net",
        "eval/longmemeval",
        "frontend/tui",
        "infra/channels",
        "infra/control",
        "infra/persistence",
        "infra/providers",
        "memory2",
        "migrations",
        "plugin_packages/curated_feeds",
        "plugins/akasha",
        "plugins/default_memory",
        "plugins/drift_flow",
        "plugins/proactive_flow",
        "plugins/wake_proactive",
        "proactive_v2",
        "session",
        "utils",
    )
    missing = []
    empty = []
    for relative in required:
        owner = ROOT / relative
        if not owner.is_dir():
            missing.append(relative)
        elif not any(path.suffix == ".py" for path in owner.iterdir() if path.is_file()):
            empty.append(relative)
    assert missing == []
    assert empty == []
