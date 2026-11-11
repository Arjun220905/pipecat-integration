"""Portable launcher lifecycle plus real Windows lock, ACL, and process checks."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from scripts import manage


class WindowsLauncherTests(unittest.TestCase):
    def test_container_start_stop_and_cleanup_retry_without_app_imports(self):
        original_directory = Path.cwd()

        def main():
            try:
                manage.main()
            finally:
                os.chdir(original_directory)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / ".run"
            run.mkdir()
            (root / ".env.example").write_text("BRIDGE_TOKEN=\n")
            commands = []
            cleanup_fails = True

            def compose(*args, **kwargs):
                commands.append(args)
                return subprocess.CompletedProcess(
                    [], 1 if args[0] == "run" and cleanup_fails else 0, stdout="container-id"
                )

            with (
                patch.object(manage, "ROOT", root),
                patch.object(manage, "RUN", run),
                patch.object(manage, "WINDOWS", True),
                patch.object(manage, "protect"),
                patch.object(manage, "manager_lock", nullcontext),
                patch.object(manage, "setup"),
                patch.object(manage.sys, "prefix", str(root / ".venv")),
                patch.object(manage, "compose", side_effect=compose),
                patch.object(manage, "get_json", side_effect=lambda _: manage.read_state()),
                patch.object(manage.shutil, "which", return_value="docker.exe"),
                patch.object(
                    manage.subprocess,
                    "run",
                    return_value=subprocess.CompletedProcess([], 0, stdout="linux\n"),
                ),
                patch.dict(
                    os.environ,
                    {
                        "MEETSTREAM_API_KEY": "test",
                        "GOOGLE_API_KEY": "test",
                        "DEEPGRAM_API_KEY": "test",
                        "PUBLIC_URL": "https://bridge.example.test",
                        "NGROK_AUTHTOKEN": "",
                        "RUN_MODE": "container",
                    },
                    clear=True,
                ),
                patch.dict(
                    sys.modules, {"meetstream_pipecat.bots": None, "meetstream_pipecat.store": None}
                ),
            ):
                manage.start(argparse.Namespace(meeting_url=None, port=0, container=True))
                self.assertTrue(manage.read_state()["container"])
                self.assertIn(("up", "-d", "--build"), commands)
                with patch.object(sys, "argv", ["manage.py", "stop"]):
                    with self.assertRaises(SystemExit) as exc:
                        main()
                    self.assertEqual(exc.exception.code, 1)
                    self.assertTrue(manage.read_state())
                    cleanup_fails = False
                    main()
                self.assertEqual(manage.read_state(), {})
                self.assertIn(("down",), commands)

    def test_windows_stale_pid_is_not_killed(self):
        with (
            patch.object(manage, "WINDOWS", True),
            patch.object(
                manage,
                "powershell",
                return_value=subprocess.CompletedProcess([], 0, stdout="unrelated.exe"),
            ),
            patch.object(manage.subprocess, "run") as run,
        ):
            manage.terminate({"pid": 123, "marker": "unique-instance"})
            run.assert_not_called()

    @unittest.skipUnless(sys.platform == "win32", "requires native Windows")
    def test_real_windows_lock_excludes_second_process(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(manage, "RUN", Path(directory)),
        ):
            code = (
                "from pathlib import Path; from scripts import manage; "
                f"manage.RUN=Path({directory!r}); "
                "print('waiting',flush=True); "
                "\nwith manage.manager_lock(): print('acquired',flush=True)"
            )
            with manage.manager_lock():
                child = subprocess.Popen(
                    [sys.executable, "-c", code], cwd=manage.ROOT, stdout=subprocess.PIPE, text=True
                )
                self.addCleanup(lambda: child.kill() if child.poll() is None else None)
                self.assertEqual(child.stdout.readline().strip(), "waiting")
                time.sleep(0.2)
                self.assertIsNone(child.poll())
            output, _ = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 0)
            self.assertEqual(output.strip(), "acquired")

    @unittest.skipUnless(sys.platform == "win32", "requires native Windows")
    def test_real_windows_private_settings_and_owned_process(self):
        with tempfile.TemporaryDirectory(prefix="bridge space '") as directory:
            run = Path(directory)
            native_powershell = manage.powershell

            def checked_powershell(script):
                result = native_powershell(script)
                self.assertEqual(result.returncode, 0, result.stderr)
                return result

            with (
                patch.object(manage, "RUN", run),
                patch.object(manage, "powershell", side_effect=checked_powershell),
            ):
                manage.protect(run)
                env = run / "settings-test.env"
                env.write_text("PUBLIC_URL=\n")
                manage.protect(env)
                manage.set_setting(env, "PUBLIC_URL", "https://example.test")
                literal = str(env).replace("'", "''")
                result = manage.powershell(
                    f"$acl=Get-Acl -LiteralPath '{literal}'; "
                    "$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User; "
                    "if (@($acl.Access).Count -ne 1 -or "
                    "$acl.Access[0].IdentityReference.Translate([Security.Principal.SecurityIdentifier]) -ne $sid) {exit 1}"
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                marker = "windows-test-" + str(time.time_ns())
                child = manage.launch(
                    [sys.executable, "-c", "import time; time.sleep(60)", marker],
                    marker,
                    "child.log",
                )
                try:
                    self.assertTrue(manage.owned(child))
                finally:
                    manage.terminate(child)
                self.assertFalse(manage.owned(child))

    @unittest.skipUnless(sys.platform == "win32", "requires native Windows")
    def test_clean_windows_cmd_bootstrap(self):
        with tempfile.TemporaryDirectory(prefix="bridge bootstrap ") as directory:
            root = Path(directory)
            for name in (
                "scripts/manage.py",
                "start.cmd",
                "status.cmd",
                "requirements-launcher.lock",
                "pyproject.toml",
                "uv.lock",
                ".env.example",
            ):
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(manage.ROOT / name, target)
            env = dict(os.environ)
            for key in (
                "BRIDGE_TOKEN",
                "DEPLOYMENT_ID",
                "MEETSTREAM_API_KEY",
                "GOOGLE_API_KEY",
                "DEEPGRAM_API_KEY",
                "NGROK_AUTHTOKEN",
                "PUBLIC_URL",
                "RUN_MODE",
            ):
                env.pop(key, None)
            result = subprocess.run(
                f'call "{root / "start.cmd"}"',
                shell=True,
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                timeout=180,
            )
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertTrue((root / ".env").exists())
            self.assertIn(".\\start.cmd", result.stderr)
            self.assertTrue(
                "Fill in" in result.stderr or "Docker Desktop" in result.stderr, result.stderr
            )
            runtime = root / ".venv/Scripts/python.exe"
            check = subprocess.run(
                [
                    str(runtime),
                    "-c",
                    "import dotenv,importlib.util; assert importlib.util.find_spec('pipecat') is None",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(check.returncode, 0, check.stderr)
            status = subprocess.run(
                f'call "{root / "status.cmd"}"',
                shell=True,
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(status.returncode, 1, status.stderr)
            self.assertEqual(status.stdout.strip(), "Stopped")

    @unittest.skipUnless(sys.platform == "win32", "requires native Windows")
    def test_cmd_entrypoint_from_path_with_spaces(self):
        with tempfile.TemporaryDirectory(prefix="bridge space ") as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            for name in ("start", "stop", "status"):
                (root / (name + ".cmd")).write_bytes((manage.ROOT / (name + ".cmd")).read_bytes())
            (root / "scripts/manage.py").write_text(
                "import json,sys; print(json.dumps(sys.argv[1:])); sys.exit(7)"
            )
            for name in ("start", "stop", "status"):
                command = f'call "{root / (name + ".cmd")}" "https://example.test/j/1?pwd=a&x=b"'
                result = subprocess.run(command, shell=True, capture_output=True, text=True)
                self.assertEqual(result.returncode, 7, result.stderr)
                self.assertEqual(
                    json.loads(result.stdout), [name, "https://example.test/j/1?pwd=a&x=b"]
                )


if __name__ == "__main__":
    unittest.main()
