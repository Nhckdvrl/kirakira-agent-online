"""TOML and environment configuration tests."""

import os
import tempfile
import unittest
from pathlib import Path

from agent.config import config_value, load_toml_config


class ConfigTests(unittest.TestCase):
    def test_toml_loads_nested_values_and_expands_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            path.write_text(
                '[llm.main]\nmodel = "deepseek-v4-flash"\napi_key = "${TEST_AGENT_KEY}"\n',
                encoding="utf-8",
            )
            previous = os.environ.get("TEST_AGENT_KEY")
            os.environ["TEST_AGENT_KEY"] = "secret-for-test"
            try:
                config = load_toml_config(path)
            finally:
                if previous is None:
                    os.environ.pop("TEST_AGENT_KEY", None)
                else:
                    os.environ["TEST_AGENT_KEY"] = previous

            self.assertEqual(
                config_value(config, "llm", "main", "model"),
                "deepseek-v4-flash",
            )
            self.assertEqual(
                config_value(config, "llm", "main", "api_key"),
                "secret-for-test",
            )

    def test_missing_referenced_environment_variable_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.toml"
            path.write_text('token = "${DEFINITELY_MISSING_KIRAKIRA_VAR}"\n')
            os.environ.pop("DEFINITELY_MISSING_KIRAKIRA_VAR", None)

            with self.assertRaises(RuntimeError):
                load_toml_config(path)


if __name__ == "__main__":
    unittest.main()
