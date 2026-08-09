"""The audit checkout is never a runtime or build dependency."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_runtime_entrypoints_do_not_import_reference() -> None:
    for owner in ("agent", "bootstrap", "bus", "core", "infra", "session"):
        for path in (ROOT / owner).rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            assert "from Reference" not in source
            assert "import Reference" not in source
            assert 'Path("Reference")' not in source
