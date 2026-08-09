"""Kirakira Agent learning harness module."""

import asyncio
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class CliTests(unittest.TestCase):
    def test_setup_rejects_telegram_display_name(self):
        import click
        from bootstrap.setup_wizard import _normalize_telegram_identity

        self.assertEqual(_normalize_telegram_identity("@jackdjjiwo"), "jackdjjiwo")
        self.assertEqual(_normalize_telegram_identity("1862986856"), "1862986856")
        with self.assertRaises(click.BadParameter):
            _normalize_telegram_identity("Xin-Yi Mae")

    def test_setup_rejects_bot_group_and_mismatched_telegram_targets(self):
        from bootstrap.setup_wizard import _validate_telegram_chat_target

        class Response:
            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        class Client:
            def __init__(self, chat):
                self.chat = chat

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def get(self, url, **_kwargs):
                if url.endswith("/getMe"):
                    return Response({"ok": True, "result": {"id": 8641833384}})
                return Response({"ok": True, "result": self.chat})

        with mock.patch(
            "httpx.Client",
            return_value=Client(
                {"id": 8641833384, "type": "private", "username": "bot"}
            ),
        ):
            self.assertIn(
                "机器人自己的 ID",
                _validate_telegram_chat_target("token", "8641833384") or "",
            )
        with mock.patch(
            "httpx.Client",
            return_value=Client(
                {"id": -1001, "type": "group", "username": "jackdjjiwo"}
            ),
        ):
            self.assertIn(
                "私聊", _validate_telegram_chat_target("token", "-1001") or ""
            )
        with mock.patch(
            "httpx.Client",
            return_value=Client(
                {"id": 999, "type": "private", "username": "someone_else"}
            ),
        ):
            self.assertIn(
                "不一致",
                _validate_telegram_chat_target(
                    "token", "999", "jackdjjiwo"
                )
                or "",
            )
        with mock.patch(
            "httpx.Client",
            return_value=Client(
                {"id": 1862986856, "type": "private", "username": "jackdjjiwo"}
            ),
        ):
            self.assertIsNone(
                _validate_telegram_chat_target(
                    "token", "1862986856", "jackdjjiwo"
                )
            )

    def test_reference_style_gateway_maps_to_full_service(self):
        from bootstrap.main import main

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.toml"
            config.write_text(
                '[runtime]\nworkspace = "workspace"\n', encoding="utf-8"
            )
            with mock.patch("bootstrap.app.main") as runtime_main:
                main(["gateway", "--config", str(config)])

            runtime_main.assert_called_once()
            runtime_args = runtime_main.call_args.args[0]
            self.assertIn("--serve", runtime_args)
            self.assertIn(str(config.resolve()), runtime_args)

    def test_reference_style_default_entry_uses_supervisor(self):
        from bootstrap.main import main

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.toml"
            workspace = root / "workspace"
            config.write_text(
                '[runtime]\nworkspace = "%s"\n' % workspace,
                encoding="utf-8",
            )
            with mock.patch(
                "agent.supervisor.run_supervisor", return_value=0
            ) as run_supervisor:
                with self.assertRaises(SystemExit) as exited:
                    main(["--config", str(config)])

            self.assertEqual(exited.exception.code, 0)
            run_supervisor.assert_called_once_with(
                config_path=config.resolve(), workspace=workspace.resolve()
            )

    def test_reference_style_supervise_rejects_unowned_flags(self):
        from bootstrap.main import main

        with self.assertRaises(SystemExit) as exited:
            main(["supervise", "--unknown", "value"])

        self.assertIn("supervise 不支持参数", str(exited.exception))

    def test_reference_style_init_creates_config_and_workspace(self):
        from bootstrap.main import main

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.toml"
            workspace = root / "workspace"
            main(
                [
                    "init",
                    "--config",
                    str(config),
                    "--workspace",
                    str(workspace),
                ]
            )

            self.assertTrue(config.is_file())
            self.assertIn(str(workspace), config.read_text(encoding="utf-8"))
            self.assertTrue((workspace / "proactive" / "inbox" / "README.md").is_file())
            self.assertTrue((workspace / "drift" / "skills" / "explore-curiosity" / "SKILL.md").is_file())

    def test_reference_style_setup_renders_supported_chains(self):
        from click.testing import CliRunner
        from bootstrap.setup_wizard import run_setup_wizard

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.toml"
            workspace = root / "workspace"

            @__import__("click").command()
            def setup_command():
                run_setup_wizard(config, workspace)

            result = CliRunner().invoke(
                setup_command,
                input=(
                    "deepseek-chat\n"
                    "https://api.deepseek.com/v1\n"
                    "secret-key\n"
                    "128000\n"
                    "n\n"
                    "n\n"
                    "n\n"
                ),
            )
            self.assertEqual(result.exit_code, 0, result.output)
            rendered = config.read_text(encoding="utf-8")
            self.assertIn("[channels.chat]", rendered)
            self.assertIn("[proactive.target]", rendered)
            self.assertIn("[proactive.drift]", rendered)
            self.assertIn("KIRAKIRA_MAIN_API_KEY=secret-key", (root / ".env").read_text())

    def test_setup_renderer_wires_all_reference_channels(self):
        from bootstrap.setup_wizard import WizardAnswers, _render_config, _render_env

        answers = WizardAnswers(
            workspace=Path("/tmp/kirakira-test"),
            model="model",
            base_url="https://model.invalid/v1",
            api_key="main-secret",
            telegram_enabled=True,
            telegram_token="tg-secret",
            telegram_allow_from=["alice"],
            qqbot_enabled=True,
            qqbot_app_id="qq-app",
            qqbot_client_secret="qqbot-secret",
            qqbot_user_openid="openid-1",
            qq_enabled=True,
            qq_bot_uin="10000",
            qq_allow_from=["10001"],
            qq_access_token="onebot-secret",
            proactive_enabled=True,
            proactive_channel="qqbot",
            proactive_chat_id="c2c:openid-1",
        )
        rendered = _render_config(answers)
        secrets = _render_env(answers)
        self.assertIn("[channels.telegram]", rendered)
        self.assertIn("[channels.qqbot]", rendered)
        self.assertIn("[channels.qq]", rendered)
        self.assertIn('channel = "qqbot"', rendered)
        self.assertIn('chat_id = "c2c:openid-1"', rendered)
        self.assertIn("TELEGRAM_BOT_TOKEN=tg-secret", secrets)
        self.assertIn("QQBOT_CLIENT_SECRET=qqbot-secret", secrets)
        self.assertIn("ONEBOT_ACCESS_TOKEN=onebot-secret", secrets)

    def test_proactive_target_auto_enables_builtin_channel(self):
        from bootstrap.app import build_runtime

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                config = workdir / "config.toml"
                config.write_text(
                    """
[llm.main]
model = "fake"
base_url = "http://example.test/v1"

[channels.chat]
channel_name = "push-web"

[proactive]
enabled = true

[proactive.target]
channel = "push-web"
chat_id = "u1"
""",
                    encoding="utf-8",
                )
                runtime = await build_runtime(workdir, config_path=config)
                try:
                    self.assertIsNotNone(runtime.channel_host)
                    self.assertIn(
                        "push-web",
                        [channel.name for channel in runtime.channel_host.channels],
                    )
                finally:
                    await runtime.stop_background([])

        asyncio.run(scenario())

    def test_cli_mode_selection(self):
        from bootstrap.app import choose_cli_mode

        self.assertEqual(
            choose_cli_mode(
                stdin_isatty=True,
                stdout_isatty=True,
                textual_available=True,
            ),
            "tui",
        )
        self.assertEqual(
            choose_cli_mode(
                stdin_isatty=False,
                stdout_isatty=True,
                textual_available=True,
            ),
            "plain",
        )
        self.assertEqual(
            choose_cli_mode(
                force_plain=True,
                stdin_isatty=True,
                stdout_isatty=True,
                textual_available=True,
            ),
            "plain",
        )
        self.assertEqual(choose_cli_mode(force_tui=True), "tui")

    def test_cli_reports_missing_env(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env.pop("MODEL_ID", None)
            env.pop("OPENAI_COMPATIBLE_BASE_URL", None)
            env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
            proc = subprocess.run(
                [sys.executable, "-m", "kirakira_agent"],
                input="/exit\n",
                text=True,
                capture_output=True,
                cwd=tmp,
                env=env,
                timeout=10,
            )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("MODEL_ID", proc.stderr + proc.stdout)

    def test_cli_tools_command_starts_and_exits(self):
        env = os.environ.copy()
        env["MODEL_ID"] = "fake"
        env["OPENAI_COMPATIBLE_BASE_URL"] = "http://example.test/v1"
        env["OPENAI_COMPATIBLE_API_KEY"] = ""
        proc = subprocess.run(
            [sys.executable, "-m", "kirakira_agent"],
            input="/tools\n/exit\n",
            text=True,
            capture_output=True,
            env=env,
            timeout=10,
        )

        self.assertEqual(proc.returncode, 0)
        self.assertIn("bash", proc.stdout)
        self.assertIn("read_file", proc.stdout)


if __name__ == "__main__":
    unittest.main()
