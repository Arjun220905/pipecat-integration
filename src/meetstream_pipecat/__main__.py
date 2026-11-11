import argparse
import json
import logging
import os
import re
import signal
import sys
import time

import httpx
from dotenv import load_dotenv

from .bots import bot_payload as bot_payload


def configure_logging():
    secrets = [
        value
        for key, value in os.environ.items()
        if value and ("API_KEY" in key or key.endswith("TOKEN"))
    ]

    def clean(message):
        for secret in secrets:
            message = message.replace(secret, "[redacted]")
        return re.sub(r'([?&]token=)[^\s"\]]+', r"\1[redacted]", message)

    class Formatter(logging.Formatter):
        def format(self, record):
            return json.dumps(
                {
                    "time": time.time(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": clean(record.getMessage()),
                }
            )

    handler = logging.StreamHandler()
    handler.setFormatter(Formatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    from loguru import logger

    logger.remove()

    def redact(record):
        record["message"] = clean(record["message"])
        record["exception"] = None
        return True

    logger.add(sys.stderr, level="INFO", serialize=True, filter=redact)


def main():
    load_dotenv(".env")
    parser = argparse.ArgumentParser(description="MeetStream × Pipecat meeting bridge")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve")
    serve.add_argument("--host", default=os.getenv("HOST", "127.0.0.1"))
    serve.add_argument("--port", default=int(os.getenv("PORT", "8000")), type=int)
    serve.add_argument("--instance-id", default=os.getenv("INSTANCE_ID"), help=argparse.SUPPRESS)
    join = commands.add_parser("join")
    join.add_argument("meeting_url")
    join.add_argument("--tenant", default="owner")
    join.add_argument("--name", default=os.getenv("BOT_NAME", "MeetStream Pipecat"))
    join.add_argument(
        "--max-seconds", type=int, default=int(os.getenv("MAX_MEETING_SECONDS", "3600"))
    )
    stop = commands.add_parser("stop")
    stop.add_argument("bot_id", nargs="?")
    commands.add_parser("reaper", help="Continuously reconcile orphaned and expired meetings")
    commands.add_parser("reconcile", help="Run cleanup once, including interrupted creations")
    commands.add_parser("sweep", help="Stateless expiry cleanup, including after whole-host loss")
    args = parser.parse_args()
    try:
        if args.command == "serve":
            import uvicorn

            from .server import create_app

            for key in (
                "BRIDGE_TOKEN",
                "DEEPGRAM_API_KEY",
                "GOOGLE_API_KEY",
                "MEETSTREAM_API_KEY",
                "DEPLOYMENT_ID",
            ):
                if not os.getenv(key):
                    raise ValueError(f"Set {key} in .env")
            if not re.fullmatch(r"[0-9a-f]{32}", os.environ["DEPLOYMENT_ID"]):
                raise ValueError("DEPLOYMENT_ID must be a persistent UUID hex string")
            configure_logging()
            uvicorn.run(
                create_app(
                    os.environ["BRIDGE_TOKEN"],
                    os.getenv("BOT_NAME", "MeetStream Pipecat"),
                    instance_id=args.instance_id,
                    on_fatal=lambda: os.kill(os.getpid(), signal.SIGTERM),
                ),
                host=args.host,
                port=args.port,
                access_log=False,
                log_level="warning",
                ws_max_size=300_000,
                ws_max_queue=4,
                ws_ping_interval=20,
                ws_ping_timeout=10,
                limit_concurrency=64,
                backlog=64,
                timeout_graceful_shutdown=20,
            )
        elif args.command == "join":
            from uuid import uuid4

            local = os.getenv("BRIDGE_LOCAL_URL", "http://127.0.0.1:" + os.getenv("PORT", "8000"))
            response = httpx.post(
                local + "/meetings",
                headers={
                    "Authorization": "Bearer " + os.environ["BRIDGE_TOKEN"],
                    "Idempotency-Key": uuid4().hex,
                },
                json={
                    "meeting_url": args.meeting_url,
                    "tenant": args.tenant,
                    "max_seconds": args.max_seconds,
                    "bot_name": args.name,
                },
                timeout=15,
            )
            if response.status_code not in (200, 201):
                raise ValueError(
                    f"Bridge could not create meeting (HTTP {response.status_code}); check readiness and private logs"
                )
            print(f"Bot created: {response.json()['bot_id']}. Admit it, then speak to it.")
        elif args.command == "stop":
            from pathlib import Path

            if not os.getenv("CONTAINERIZED"):
                state_path = Path(os.getenv("MEETSTREAM_STATE_DIR", ".run")) / "processes.json"
                if state_path.exists() and json.loads(state_path.read_text()).get("container"):
                    raise ValueError(
                        "Use ./stop to clean up the container deployment and its durable registry"
                    )
            from .bots import stop_bots

            pending = stop_bots(args.bot_id)
            if pending:
                raise ValueError(
                    "Meeting cleanup still pending; rerun ./stop: " + ", ".join(pending)
                )
            print("No registered meeting bots remain.")
        else:
            from .bots import api_client, reconcile_once, sweep_remote
            from .store import Store

            configure_logging()
            if args.command == "sweep":
                with api_client() as client:
                    pending = sweep_remote(client, os.environ["DEPLOYMENT_ID"])
                if pending:
                    raise ValueError("Remote expiry cleanup pending; rerun sweep")
                return
            store = Store()
            if args.command == "reconcile":
                if reconcile_once(store):
                    raise ValueError("Remote cleanup pending; retained for retry")
                return
            stopping = False

            def stop_reaper(*_):
                nonlocal stopping
                stopping = True

            signal.signal(signal.SIGTERM, stop_reaper)
            signal.signal(signal.SIGINT, stop_reaper)
            while not stopping:
                try:
                    reconcile_once(store)
                except Exception:
                    store.set_meta("cleanup_pending", 1)
                    logging.error("Meeting cleanup failed; records retained for retry")
                for _ in range(10):
                    if stopping:
                        break
                    time.sleep(0.5)
    except (KeyError, ValueError, OSError, httpx.HTTPError) as exc:
        message = (
            str(exc)
            if isinstance(exc, ValueError)
            else f"{type(exc).__name__}; check private logs and configuration."
        )
        parser.exit(1, message + "\n")


if __name__ == "__main__":
    main()
