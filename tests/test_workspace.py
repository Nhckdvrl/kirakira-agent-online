"""Workspace resolution precedence tests."""

import os
import unittest
from pathlib import Path
from unittest import mock

from bootstrap.app import resolve_workspace


class ResolveWorkspaceTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch.dict(os.environ, {}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop("KIRAKIRA_WORKSPACE", None)

    def test_defaults_when_nothing_configured(self) -> None:
        default = Path("/tmp/default-ws")
        self.assertEqual(resolve_workspace(None, {}, default=default), default)

    def test_config_value_is_used(self) -> None:
        resolved = resolve_workspace(
            None, {"runtime": {"workspace": "/tmp/from-config"}}, default=Path("/tmp/d")
        )
        self.assertEqual(resolved, Path("/tmp/from-config").resolve())

    def test_env_overrides_config(self) -> None:
        os.environ["KIRAKIRA_WORKSPACE"] = "/tmp/from-env"
        resolved = resolve_workspace(
            None, {"runtime": {"workspace": "/tmp/from-config"}}, default=Path("/tmp/d")
        )
        self.assertEqual(resolved, Path("/tmp/from-env").resolve())

    def test_cli_overrides_env_and_config(self) -> None:
        os.environ["KIRAKIRA_WORKSPACE"] = "/tmp/from-env"
        resolved = resolve_workspace(
            "/tmp/from-cli",
            {"runtime": {"workspace": "/tmp/from-config"}},
            default=Path("/tmp/d"),
        )
        self.assertEqual(resolved, Path("/tmp/from-cli").resolve())

    def test_blank_values_fall_through(self) -> None:
        os.environ["KIRAKIRA_WORKSPACE"] = "   "
        resolved = resolve_workspace(
            "", {"runtime": {"workspace": "/tmp/from-config"}}, default=Path("/tmp/d")
        )
        self.assertEqual(resolved, Path("/tmp/from-config").resolve())

    def test_home_is_expanded(self) -> None:
        resolved = resolve_workspace(
            "~/kirakira-ws", {}, default=Path("/tmp/d")
        )
        self.assertEqual(resolved, Path.home() / "kirakira-ws")
        self.assertTrue(resolved.is_absolute())


if __name__ == "__main__":
    unittest.main()
