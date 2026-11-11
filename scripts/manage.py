"""Small Windows/macOS/Linux launcher. Stdlib bootstrap; no shell activation or daemon dependency."""

import argparse
import base64
import errno
import hashlib
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
# ponytail: Docker supplies supervision; native mode stays a local development convenience.
RUN = ROOT / ".run"
WINDOWS = sys.platform == "win32"
PYTHON = ROOT / (".venv/Scripts/python.exe" if WINDOWS else ".venv/bin/python")
START, STOP, STATUS = (
    f".\\{name}.cmd" if WINDOWS else f"./{name}" for name in ("start", "stop", "status")
)
LOCAL_HTTP = build_opener(ProxyHandler({}))


def powershell(script):
    script = (
        "$ErrorActionPreference='Stop'; [Console]::OutputEncoding=[Text.Encoding]::UTF8; " + script
    )
    return subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-EncodedCommand",
            base64.b64encode(script.encode("utf-16le")).decode("ascii"),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        # PowerShell 7's inherited module path can break Windows PowerShell 5.1 modules.
        env={key: value for key, value in os.environ.items() if key.lower() != "psmodulepath"},
    )


def protect(path):
    if not WINDOWS:
        path.chmod(0o700 if path.is_dir() else 0o600)
        return
    # Windows chmod does not restrict readers; use the current user's NTFS ACL.
    kind = "Directory" if path.is_dir() else "File"
    inheritance = "'ContainerInherit,ObjectInherit'" if path.is_dir() else "'None'"
    literal = str(path).replace("'", "''")
    result = powershell(
        "$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User; "
        f"$acl=New-Object Security.AccessControl.{kind}Security; "
        "$acl.SetOwner($sid); $acl.SetAccessRuleProtection($true,$false); "
        f"$rule=New-Object Security.AccessControl.FileSystemAccessRule($sid,'FullControl',{inheritance},'None','Allow'); "
        f"$acl.AddAccessRule($rule); Set-Acl -LiteralPath '{literal}' -AclObject $acl"
    )
    if result.returncode:
        raise ValueError(
            "Could not restrict private settings/state to your Windows account. Use a local NTFS project folder."
        )


@contextmanager
def manager_lock():
    with (RUN / "manager.lock").open("a+b") as lock:
        if WINDOWS:
            import msvcrt

            lock.seek(0, 2)
            if not lock.tell():
                lock.write(b"\0")
                lock.flush()
            lock.seek(0)
            while True:
                try:
                    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EACCES, errno.EDEADLK):
                        raise
                    time.sleep(0.1)
        else:
            import fcntl

            fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            if WINDOWS:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def setup(command):
    """The system Python can be old; only the application needs Python 3.11–3.13."""
    if not PYTHON.exists():
        if command != "start":
            if read_state() or (RUN / "bots.json").exists():
                raise ValueError(
                    f"Runtime is missing but saved state exists. Run {START} to restore it, then {STOP}."
                )
            print("Not installed; nothing is running.")
            raise SystemExit(0)
        candidate = None
        for command_line in (
            [sys.executable],
            ["py", "-3.13"],
            ["py", "-3.12"],
            ["py", "-3.11"],
            ["python3.13"],
            ["python3.12"],
            ["python3.11"],
            ["python3"],
            ["python"],
        ):
            executable = shutil.which(command_line[0])
            if executable:
                check = subprocess.run(
                    [
                        executable,
                        *command_line[1:],
                        "-c",
                        "import sys; sys.exit(not ((3,11) <= sys.version_info[:2] < (3,14)))",
                    ]
                )
                if check.returncode == 0:
                    candidate = [executable, *command_line[1:]]
                    break
        if not candidate:
            raise ValueError(f"Install Python 3.11, 3.12, or 3.13, then run {START} again.")
        subprocess.run([*candidate, "-m", "venv", str(ROOT / ".venv")], check=True)
        (RUN / "installed.sha256").unlink(missing_ok=True)
    if command == "start":
        digest = hashlib.sha256(
            b"".join(
                (ROOT / name).read_bytes()
                for name in (
                    "pyproject.toml",
                    "uv.lock",
                    "requirements-launcher.lock" if WINDOWS else "requirements.lock",
                )
            )
        ).hexdigest()
        stamp = RUN / "installed.sha256"
        if not stamp.exists() or stamp.read_text() != digest:
            print("Installing project dependencies (first run or dependency change)…", flush=True)
            with (RUN / "install.log").open("ab") as log:
                subprocess.run(
                    [
                        str(PYTHON),
                        "-m",
                        "pip",
                        "install",
                        "--require-hashes",
                        "-r",
                        str(
                            ROOT
                            / ("requirements-launcher.lock" if WINDOWS else "requirements.lock")
                        ),
                    ],
                    check=True,
                    stdout=log,
                    stderr=log,
                )
                if not WINDOWS:
                    subprocess.run(
                        [str(PYTHON), "-m", "pip", "install", "--no-deps", "-e", str(ROOT)],
                        check=True,
                        stdout=log,
                        stderr=log,
                    )
            stamp.write_text(digest)


def read_state():
    path = RUN / "processes.json"
    return json.loads(path.read_text()) if path.exists() else {}


def save_state(state):
    path = RUN / "processes.tmp"
    path.write_text(json.dumps(state))
    path.replace(RUN / "processes.json")


def owned(process):
    """A stale PID must never kill an unrelated process that inherited its number."""
    if not process:
        return False
    if WINDOWS:
        result = powershell(
            f"(Get-CimInstance Win32_Process -Filter 'ProcessId = {int(process['pid'])}').CommandLine"
        )
        if result.returncode:
            raise ValueError(
                "Could not verify the background process owner; keep saved state and retry."
            )
    else:
        result = subprocess.run(
            ["ps", "-p", str(process["pid"]), "-o", "args="], capture_output=True, text=True
        )
    return result.returncode == 0 and process["marker"] in result.stdout


def compose(*arguments, **options):
    project = "meetstream-pipecat-" + hashlib.sha256(str(ROOT).encode()).hexdigest()[:10]
    return subprocess.run(
        ["docker", "compose", "-p", project, "-f", str(ROOT / "compose.yaml"), *arguments],
        cwd=ROOT,
        check=options.pop("check", True),
        **options,
    )


def deployment_active(state):
    if state.get("container"):
        result = compose(
            "ps", "--status", "running", "-q", "bridge", check=False, capture_output=True, text=True
        )
        return result.returncode == 0 and bool(result.stdout.strip())
    return owned(state.get("server"))


def join_meeting(state, meeting_url):
    if state.get("container"):
        compose("exec", "-T", "bridge", "meetstream-pipecat", "join", meeting_url)
    else:
        os.environ["BRIDGE_LOCAL_URL"] = f"http://127.0.0.1:{state['port']}"
        subprocess.run([str(PYTHON), "-m", "meetstream_pipecat", "join", meeting_url], check=True)


def launch(args, marker, logfile):
    with (RUN / logfile).open("ab") as log:
        child = subprocess.Popen(
            args,
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=not WINDOWS,
            creationflags=(subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP)
            if WINDOWS
            else 0,
        )
    return {"pid": child.pid, "marker": marker}


def terminate(process):
    if not owned(process):
        return
    if WINDOWS:
        subprocess.run(
            ["taskkill.exe", "/PID", str(process["pid"]), "/T", "/F"], capture_output=True
        )
        if owned(process):
            raise ValueError("Could not stop the owned process; rerun the stop command.")
        return
    try:
        os.killpg(process["pid"], signal.SIGTERM)
        deadline = time.monotonic() + 20
        while owned(process) and time.monotonic() < deadline:
            time.sleep(0.1)
        if owned(process):
            os.killpg(process["pid"], signal.SIGKILL)
    except ProcessLookupError:
        pass


def get_json(url):
    with LOCAL_HTTP.open(url, timeout=1) as response:
        return json.load(response)


def set_setting(path, key, value):
    from dotenv import set_key

    if not WINDOWS:
        set_key(path, key, value)
        return
    # dotenv replaces files; stage in the private directory to preserve Windows ACLs.
    temporary = RUN / "settings.env"
    try:
        shutil.copyfile(path, temporary)
        set_key(temporary, key, value)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def start(args):
    from dotenv import load_dotenv

    env = ROOT / ".env"
    if not env.exists():
        shutil.copyfile(ROOT / ".env.example", env)
    protect(env)
    load_dotenv(env)
    if not os.getenv("BRIDGE_TOKEN"):
        os.environ["BRIDGE_TOKEN"] = secrets.token_urlsafe(32)
        set_setting(env, "BRIDGE_TOKEN", os.environ["BRIDGE_TOKEN"])
    container = (
        getattr(args, "container", False) or os.getenv("RUN_MODE", "container") == "container"
    )
    if getattr(args, "native", False):
        container = False
    if WINDOWS and not container:
        raise ValueError(
            "Windows uses Docker Desktop (Linux containers, Hyper-V backend); remove --native and use --container."
        )
    if getattr(args, "container", False) or getattr(args, "native", False):
        set_setting(env, "RUN_MODE", "container" if container else "native")
    if container and not shutil.which("docker"):
        raise ValueError("Install and start Docker Desktop in Linux-container mode.")
    if not os.getenv("DEPLOYMENT_ID"):
        os.environ["DEPLOYMENT_ID"] = uuid4().hex
        set_setting(env, "DEPLOYMENT_ID", os.environ["DEPLOYMENT_ID"])
    missing = [
        key
        for key in ("DEEPGRAM_API_KEY", "GOOGLE_API_KEY", "MEETSTREAM_API_KEY")
        if not os.getenv(key)
    ]
    if missing:
        raise ValueError("Fill in " + ", ".join(missing) + f" in .env, then rerun {START}.")
    if WINDOWS:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.OSType}}"], capture_output=True, text=True
        )
        if result.returncode or result.stdout.strip() != "linux":
            raise ValueError(
                "Start Docker Desktop with Linux containers and the Hyper-V backend (no WSL required)."
            )
    state = read_state()
    if deployment_active(state):
        print(f"Bridge already running. Use {STATUS} or {STOP}.")
        if args.meeting_url:
            get_json(f"http://127.0.0.1:{state['port']}/ready")
            join_meeting(state, args.meeting_url)
        return
    if state and bool(state.get("container")) != container:
        raise ValueError(f"Run {STOP} before changing between native and container modes.")
    if container and not WINDOWS:
        from meetstream_pipecat.bots import stop_bots
        from meetstream_pipecat.store import Store

        native_store = Store()
        if native_store.active() and stop_bots(store=native_store):
            raise ValueError(f"Old native meeting cleanup is still pending; rerun {STOP} first.")
    terminate(state.get("tunnel"))
    # Check before creating a tunnel; never claim or stop an unrelated listener.
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", args.port))
        except OSError as exc:
            raise ValueError(
                f"Port {args.port} is in use. Stop its owner or use {START} --port PORT."
            ) from exc
    public = os.getenv("PUBLIC_URL", "")
    hostname = urlsplit(public).hostname or ""
    tunnel = bool(os.getenv("NGROK_AUTHTOKEN")) and (
        not hostname or hostname.endswith((".ngrok-free.dev", ".ngrok.io", ".ngrok.app"))
    )
    if tunnel and not shutil.which("ngrok"):
        raise ValueError("Install the ngrok CLI, or set PUBLIC_URL to your own HTTPS/WSS proxy.")
    if not tunnel and (not public or hostname.endswith(".example")):
        raise ValueError(
            "Set NGROK_AUTHTOKEN for an automatic tunnel, or PUBLIC_URL for your HTTPS/WSS proxy."
        )
    if not tunnel:
        url = urlsplit(public)
        if url.scheme != "https" or not hostname or url.username or url.query or url.fragment:
            raise ValueError("PUBLIC_URL must be HTTPS without credentials, query, or fragment.")
    if len(os.environ["BRIDGE_TOKEN"]) < 16:
        raise ValueError("BRIDGE_TOKEN must contain at least 16 random characters.")
    instance = uuid4().hex
    state = {"instance_id": instance, "port": args.port, "container": container}
    os.environ["INSTANCE_ID"] = instance
    os.environ["BRIDGE_PORT"] = str(args.port)
    try:
        if tunnel:
            with socket.socket() as probe:
                try:
                    probe.bind(("127.0.0.1", 4040))
                except OSError as exc:
                    raise ValueError(
                        f"Another ngrok agent uses port 4040. Stop that agent before {START}."
                    ) from exc
            marker = str(RUN / "ngrok.log")
            state["tunnel"] = launch(
                [
                    "ngrok",
                    "http",
                    str(args.port),
                    "--inspect=false",
                    "--log",
                    marker,
                    "--log-format",
                    "json",
                ],
                marker,
                "tunnel-console.log",
            )
            save_state(state)
            deadline = time.monotonic() + 20
            while True:
                if not owned(state["tunnel"]):
                    raise ValueError(
                        "Tunnel failed. Check .run/ngrok.log (private; do not share credentials)."
                    )
                try:
                    tunnels = get_json("http://127.0.0.1:4040/api/tunnels")["tunnels"]
                    match = next(
                        (
                            t
                            for t in tunnels
                            if t.get("public_url", "").startswith("https://")
                            and str(t.get("config", {}).get("addr", "")).rstrip("/")
                            in {
                                str(args.port),
                                f"http://localhost:{args.port}",
                                f"http://127.0.0.1:{args.port}",
                            }
                        ),
                        None,
                    )
                    if match:
                        public = match["public_url"]
                        os.environ["PUBLIC_URL"] = public
                        set_setting(env, "PUBLIC_URL", public)
                        break
                except (OSError, ValueError, KeyError):
                    pass
                if time.monotonic() >= deadline:
                    raise ValueError("Tunnel did not become ready. Check .run/ngrok.log.")
                time.sleep(0.2)
        if container:
            print("Building and starting supervised containers…", flush=True)
            with (RUN / "containers.log").open("ab") as log:
                compose("up", "-d", "--build", stdout=log, stderr=log)
        else:
            state["server"] = launch(
                [
                    str(PYTHON),
                    "-m",
                    "meetstream_pipecat",
                    "serve",
                    "--port",
                    str(args.port),
                    "--instance-id",
                    instance,
                ],
                instance,
                "bridge.log",
            )
        save_state(state)
        deadline = time.monotonic() + 90
        while True:
            if not deployment_active(state):
                raise ValueError("Bridge failed to start. Check .run/bridge.log.")
            try:
                if get_json(f"http://127.0.0.1:{args.port}/health").get("instance_id") == instance:
                    get_json(f"http://127.0.0.1:{args.port}/ready")
                    break
            except (OSError, ValueError):
                pass
            if time.monotonic() >= deadline:
                raise ValueError("Bridge did not become healthy. Check .run/bridge.log.")
            time.sleep(0.2)
        print(f"Bridge running at http://127.0.0.1:{args.port}; public endpoint: {public}")
        print(f"Stop with {STOP}. Private logs: .run/bridge.log")
        if args.meeting_url:
            join_meeting(state, args.meeting_url)
    except BaseException:
        if container:
            compose(
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "reaper",
                "stop",
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            compose("down", check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        terminate(state.get("server"))
        terminate(state.get("tunnel"))
        save_state(state)  # Retain the deployment identity for cleanup retries.
        raise


def main():
    os.chdir(ROOT)
    os.umask(0o077)
    RUN.mkdir(mode=0o700, exist_ok=True)
    os.environ["MEETSTREAM_STATE_DIR"] = str(RUN)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("start", "stop", "status"))
    parser.add_argument("meeting_url", nargs="?")
    parser.add_argument("--port", type=int, default=8000)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--container", action="store_true")
    mode.add_argument("--native", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.command != "start" and args.meeting_url:
        parser.error("Only start accepts a meeting URL")
    try:
        protect(RUN)
        with manager_lock():
            setup(args.command)
        if Path(sys.prefix).resolve() != (ROOT / ".venv").resolve():
            command = [str(PYTHON), str(Path(__file__).resolve()), *sys.argv[1:]]
            if WINDOWS:
                raise SystemExit(subprocess.run(command, cwd=ROOT).returncode)
            os.execv(str(PYTHON), command)
        with manager_lock():
            if args.command == "start":
                start(args)
            elif args.command == "stop":
                from dotenv import load_dotenv

                load_dotenv(ROOT / ".env")
                state = read_state()
                if WINDOWS and not state:
                    state = {"container": True}
                pending = []
                try:
                    if state.get("container"):
                        compose("stop", "bridge")
                    else:
                        terminate(state.get("server"))
                    if state.get("container"):
                        result = compose(
                            "run", "--rm", "--no-deps", "-T", "reaper", "stop", check=False
                        )
                        if result.returncode:
                            pending = ["registered container meetings"]
                    elif WINDOWS:
                        raise ValueError("Windows cannot clean up a native deployment.")
                    else:
                        from meetstream_pipecat.bots import stop_bots

                        pending = stop_bots()
                except Exception:
                    pending = ["unconfirmed shutdown or meeting cleanup"]
                finally:
                    if state.get("container"):
                        if compose("down", check=False).returncode:
                            pending.append("container shutdown")
                    terminate(state.get("server"))
                    terminate(state.get("tunnel"))
                    if not pending:
                        (RUN / "processes.json").unlink(missing_ok=True)
                if pending:
                    save_state(state)
                    raise ValueError(f"Shutdown incomplete; rerun {STOP}: " + ", ".join(pending))
                print("Stopped. No tracked meeting bots remain.")
            else:
                state = read_state()
                active = deployment_active(state)
                healthy = False
                if active:
                    try:
                        healthy = (
                            get_json(f"http://127.0.0.1:{state['port']}/health").get("instance_id")
                            == state["instance_id"]
                        )
                        get_json(f"http://127.0.0.1:{state['port']}/ready")
                    except (OSError, ValueError, KeyError):
                        healthy = False
                    if state.get("tunnel") and not owned(state["tunnel"]):
                        healthy = False
                print("Running" if healthy else "Unhealthy" if active else "Stopped")
                if active:
                    print(f"Local: http://127.0.0.1:{state['port']} | Logs: .run/bridge.log")
                raise SystemExit(0 if healthy else 1)
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        # Do not echo subprocess arguments, environment values, or network URLs with tokens.
        message = (
            str(exc) if isinstance(exc, ValueError) else f"{type(exc).__name__}; check .run logs."
        )
        print(message, file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
