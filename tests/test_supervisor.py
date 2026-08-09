"""Reference-aligned supervisor/readiness contract tests."""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from bootstrap.runtime_readiness import RuntimeReadiness
from agent.supervisor import _SupervisorLock, _valid_commit


class SupervisorTests(unittest.TestCase):
    def test_default_entry_supervises_real_gateway_until_signal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = root / "workspace"
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            config = root / "config.toml"
            config.write_text(
                f'''[runtime]
workspace = "{workspace}"

[llm.main]
model = "fake"
base_url = "http://example.invalid/v1"
api_key = ""

[channels.chat]
enabled = true
host = "127.0.0.1"
port = {port}

[proactive]
enabled = false
''',
                encoding="utf-8",
            )
            project_root = Path(__file__).resolve().parents[1]
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(project_root / "main.py"),
                    "--config",
                    str(config),
                    "--workspace",
                    str(workspace),
                ],
                cwd=project_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            readiness = workspace / ".runtime-ready.json"
            deadline = time.monotonic() + 10
            try:
                while time.monotonic() < deadline and not readiness.exists():
                    if process.poll() is not None:
                        break
                    time.sleep(0.05)
                self.assertTrue(
                    readiness.exists(),
                    process.stdout.read() if process.poll() is not None and process.stdout else "",
                )
                process.terminate()
                output, _ = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 0, output)
                self.assertFalse((workspace / ".supervisor.pid").exists())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)

    def test_readiness_is_boot_owned_and_atomic(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            readiness = RuntimeReadiness(workspace, "boot-one")
            readiness.mark_ready()
            payload = json.loads(
                (workspace / ".runtime-ready.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                payload,
                {"bootId": "boot-one", "pid": os.getpid(), "state": "ready"},
            )
            readiness.clear()
            self.assertFalse((workspace / ".runtime-ready.json").exists())

    def test_restart_commit_requires_private_boot_identity(self):
        frame = json.dumps(
            {
                "type": "restart_commit",
                "bootId": "boot-one",
                "nonce": "secret",
                "requestId": "restart_123",
            }
        ).encode()
        self.assertTrue(_valid_commit(frame + b"\n", boot_id="boot-one", nonce="secret"))
        self.assertFalse(_valid_commit(frame + b"\n", boot_id="other", nonce="secret"))
        self.assertFalse(_valid_commit(frame + b"\n", boot_id="boot-one", nonce="wrong"))

    def test_only_one_supervisor_owns_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            first = _SupervisorLock(workspace)
            second = _SupervisorLock(workspace)
            first.acquire()
            try:
                with self.assertRaises(RuntimeError):
                    second.acquire()
            finally:
                first.release()


if __name__ == "__main__":
    unittest.main()
