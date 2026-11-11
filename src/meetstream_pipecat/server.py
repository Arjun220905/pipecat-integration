"""Registered, isolated meeting sessions with bounded failure and cleanup paths."""

import asyncio
import fcntl
import hmac
import json
import logging
import os
import time
from collections import Counter, deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from urllib.parse import urlsplit
from uuid import uuid4

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, PlainTextResponse
from pipecat.workers.runner import WorkerRunner

from .bots import api_client, bot_payload, create_reserved, reconcile_once
from .flow import AudioOverload
from .protocol import MAX_AUDIO_BYTES, ready_bot_id
from .store import Store
from .transport import MeetStreamTransport

logger = logging.getLogger(__name__)


@dataclass
class Session:
    session_id: str
    bot_id: str
    expires: float
    sockets: dict = field(default_factory=dict)
    paired: asyncio.Event = field(default_factory=asyncio.Event)
    started: asyncio.Event = field(default_factory=asyncio.Event)
    disconnected: asyncio.Event = field(default_factory=asyncio.Event)
    failed: asyncio.Event = field(default_factory=asyncio.Event)
    transport: MeetStreamTransport | None = None
    task: asyncio.Task | None = None
    reason: str = "socket_disconnected"


def create_app(
    token,
    bot_name="MeetStream Pipecat",
    agent_factory=None,
    *,
    instance_id=None,
    store=None,
    on_fatal=None,
):
    if len(token) < 16:
        raise ValueError("BRIDGE_TOKEN must be a random secret of at least 16 characters")
    if agent_factory is None:
        from .agent import create_agent

        agent_factory = create_agent
    store = store or Store()
    sessions = {}
    admission_lock = asyncio.Lock()
    creation_lock = asyncio.Lock()
    counters = Counter()
    owner = uuid4().hex
    accepting = False
    handshakes = 0
    provider_unavailable_until = 0
    max_sessions = int(os.getenv("MAX_SESSIONS", "8"))
    if not 1 <= max_sessions <= 32:
        raise ValueError("MAX_SESSIONS must be between 1 and 32")

    async def close(socket, code=1000):
        with suppress(WebSocketDisconnect, RuntimeError, OSError, TimeoutError):
            await asyncio.wait_for(socket.close(code=code), timeout=1)

    async def run_session(session):
        nonlocal provider_unavailable_until
        running = None
        runner = WorkerRunner(handle_sigint=False, check_dangling_tasks=False)
        disconnected = asyncio.create_task(session.disconnected.wait())
        failed = asyncio.create_task(session.failed.wait())
        pair = asyncio.create_task(session.paired.wait())
        error_times = deque(maxlen=3)
        try:
            done, _ = await asyncio.wait(
                [pair, disconnected, failed], timeout=30, return_when=asyncio.FIRST_COMPLETED
            )
            if pair not in done or disconnected in done or failed in done:
                session.reason = "pairing_failed"
                return
            registration = await store_call(store.get, session.session_id)
            own_name = (
                json.loads(registration["payload"]).get("bot_name", bot_name)
                if registration["payload"]
                else bot_name
            )
            hostname = urlsplit(registration["meeting_url"]).hostname or ""
            session.transport = MeetStreamTransport(
                session.sockets["control"],
                session.bot_id,
                own_name,
                require_resume_ack=hostname == "zoom.us" or hostname.endswith(".zoom.us"),
            )

            def command_failure():
                counters["meeting_command_failures_total"] += 1
                session.reason = "meeting_command_failed"
                session.failed.set()

            session.transport.output().on_command_failure = command_failure

            def overload():
                counters["audio_overload_total"] += 1
                session.reason = "audio_overload"
                session.failed.set()

            session.transport.input().on_overload = overload
            session.transport.output().on_overload = overload
            worker = agent_factory(session.transport)

            @worker.event_handler("on_pipeline_error")
            async def pipeline_error(worker, frame):
                nonlocal provider_unavailable_until
                if session.failed.is_set():
                    return
                counters["pipeline_errors_total"] += 1
                error_times.append(time.monotonic())
                provider_unavailable_until = time.time() + 30
                await store_call(
                    store.set_meta, "provider_unavailable_until", provider_unavailable_until
                )
                try:
                    await asyncio.wait_for(session.transport.input().broadcast_interruption(), 2)
                    await session.transport.output().notice(
                        "I am having trouble connecting to the voice service. Please try again shortly."
                    )
                except Exception:
                    session.failed.set()
                category = getattr(frame, "category", None)
                permanent = (
                    getattr(category, "is_permanent", False)
                    or getattr(category, "value", "") == "quota"
                )
                if (
                    frame.fatal
                    or permanent
                    or (len(error_times) == 3 and error_times[-1] - error_times[0] < 60)
                ):
                    session.reason = "provider_failure"
                    session.failed.set()

            await runner.add_workers(worker)
            running = asyncio.create_task(runner.run())
            session.started.set()
            counters["sessions_started_total"] += 1
            timeout = max(0, session.expires - time.time())
            done, _ = await asyncio.wait(
                [running, disconnected, failed],
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                session.reason = "meeting_deadline"
            elif running in done:
                session.reason = "pipeline_finished"
        except Exception:
            counters["session_errors_total"] += 1
            session.reason = "pipeline_failed"
            logger.error("Meeting pipeline failed; details withheld to protect credentials")
        finally:
            # Revoke first, even if a downstream cancellation hangs.
            try:
                await store_call(store.stopping, session.session_id, session.reason)
            except Exception:
                counters["state_failures_total"] += 1
                logger.error(
                    "Unable to persist session shutdown; cleanup must use the expired lease"
                )
            session.disconnected.set()
            if running:
                try:
                    await asyncio.wait_for(runner.cancel(reason=session.reason), 5)
                    await asyncio.wait_for(asyncio.shield(running), 10)
                except Exception:
                    running.cancel()
                    counters["shutdown_timeouts_total"] += 1
            for task in (pair, disconnected, failed):
                task.cancel()
            await asyncio.gather(pair, disconnected, failed, return_exceptions=True)
            await asyncio.gather(*(close(ws) for ws in session.sockets.values()))
            if sessions.get(session.bot_id) is session:
                del sessions[session.bot_id]
            counters["sessions_finished_total"] += 1
            logger.info("Meeting pipeline closed: %s", session.reason)

    async def store_call(function, *args):
        return await asyncio.to_thread(function, *args)

    async def maintain():
        while True:
            await store_call(store.heartbeat, owner)
            await store_call(store.set_meta, "bridge_heartbeat", time.time())
            for session in list(sessions.values()):
                row = await store_call(store.get, session.session_id)
                if not row or row["state"] != "active" or row["expires"] <= time.time():
                    session.reason = "credential_revoked_or_expired"
                    session.failed.set()
            await asyncio.sleep(2)

    async def cleanup():
        while True:
            try:
                await asyncio.to_thread(reconcile_once, store)
            except Exception:
                counters["cleanup_loop_errors_total"] += 1
                with suppress(Exception):
                    await store_call(store.set_meta, "cleanup_pending", 1)
                logger.error("Remote meeting cleanup unavailable; retained for retry")
            await asyncio.sleep(5)

    @asynccontextmanager
    async def lifespan(app):
        nonlocal accepting
        # SQLite is shared, but voice sessions must have exactly one worker.
        with (store.directory / "bridge.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            await store_call(store.recover, owner)
            background = [asyncio.create_task(maintain())]
            if os.getenv("EXTERNAL_REAPER", "false").lower() != "true":
                background.append(asyncio.create_task(cleanup()))

            def background_finished(task):
                nonlocal accepting
                if not task.cancelled() and task.exception() and accepting:
                    accepting = False
                    logger.error("Service maintenance failed; stopping for supervised restart")
                    if on_fatal:
                        on_fatal()

            for task in background:
                task.add_done_callback(background_finished)
            accepting = True
            try:
                yield
            finally:
                accepting = False
                current = list(sessions.values())
                for session in current:
                    session.reason = "service_shutdown"
                    session.failed.set()
                if current:
                    await asyncio.gather(*(s.task for s in current), return_exceptions=True)
                for task in background:
                    task.cancel()
                await asyncio.gather(*background, return_exceptions=True)

    app = FastAPI(title="MeetStream × Pipecat", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.state.sessions = sessions
    app.state.store = store
    app.state.counters = counters

    @app.get("/health")
    async def health():
        return {"status": "ok", **({"instance_id": instance_id} if instance_id else {})}

    @app.get("/ready")
    async def ready():
        try:
            heartbeat = await store_call(store.meta, "reaper_heartbeat")
            pending = await store_call(store.meta, "cleanup_pending")
            unavailable = await store_call(store.meta, "provider_unavailable_until")
            healthy = (
                accepting
                and time.time() - heartbeat < 90
                and not pending
                and time.time() >= unavailable
            )
        except Exception:
            healthy = False
        return JSONResponse(
            {"status": "ready" if healthy else "not_ready"}, status_code=200 if healthy else 503
        )

    @app.get("/metrics")
    async def metrics(request: Request):
        if not hmac.compare_digest(
            request.headers.get("authorization", "").encode(), f"Bearer {token}".encode()
        ):
            return PlainTextResponse("Unauthorized", status_code=401)
        values = dict(counters)
        values["active_sessions"] = len(sessions)
        values["audio_input_backlog_seconds"] = sum(
            s.transport.input().window.pending for s in sessions.values() if s.transport
        )
        values["audio_output_backlog_seconds"] = sum(
            s.transport.output().window.pending for s in sessions.values() if s.transport
        )
        values["cleanup_pending"] = await store_call(store.meta, "cleanup_pending")
        values["cleanup_failures_total"] = await store_call(store.meta, "cleanup_failures_total")
        values["reaper_heartbeat_timestamp_seconds"] = await store_call(
            store.meta, "reaper_heartbeat"
        )
        return PlainTextResponse(
            "".join(f"meetstream_{name} {value}\n" for name, value in sorted(values.items())),
            media_type="text/plain; version=0.0.4",
        )

    @app.post("/meetings")
    async def create_meeting(request: Request):
        if not hmac.compare_digest(
            request.headers.get("authorization", "").encode(), f"Bearer {token}".encode()
        ):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        if (await ready()).status_code != 200:
            return JSONResponse({"error": "not_ready"}, status_code=503)
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > 4096:
                return JSONResponse({"error": "request_too_large"}, status_code=413)
            body.extend(chunk)
        try:
            parameters = json.loads(body)
            if not isinstance(parameters, dict) or not isinstance(
                parameters.get("meeting_url"), str
            ):
                raise ValueError("Invalid meeting request")
            duration = parameters.get("max_seconds", int(os.getenv("MAX_MEETING_SECONDS", "3600")))
            if type(duration) is not int or not 60 <= duration <= int(
                os.getenv("MAX_MEETING_SECONDS", "3600")
            ):
                raise ValueError("Meeting exceeds configured lifetime")
            request_id = request.headers.get("idempotency-key") or uuid4().hex
            if len(request_id) != 32 or any(c not in "0123456789abcdef" for c in request_id):
                raise ValueError("Idempotency-Key must be a lowercase UUID hex string")
            async with asyncio.timeout(12), creation_lock:

                def provision():
                    row = store.get(request_id)
                    if row:
                        if (
                            row["meeting_url"] != parameters["meeting_url"]
                            or row["tenant"] != parameters.get("tenant", "owner")
                            or row["state"] not in {"creating", "active"}
                        ):
                            raise ValueError("Creation request conflicts with saved state")
                    else:
                        sid, secret = store.reserve(
                            parameters["meeting_url"],
                            parameters.get("tenant", "owner"),
                            max_seconds=duration,
                            limit=max_sessions,
                            session_id=request_id,
                        )
                        try:
                            payload = bot_payload(
                                parameters["meeting_url"],
                                session_id=sid,
                                session_token=secret,
                                expires_at=store.get(sid)["expires"],
                                bot_name=parameters.get("bot_name"),
                            )
                            store.save_payload(sid, payload)
                        except Exception:
                            store.finished(sid)
                            raise
                    try:
                        with api_client() as client:
                            bot_id = create_reserved(store, request_id, client)
                    except Exception:
                        store.stopping(request_id, "creation_not_confirmed")
                        raise
                    if store.get(request_id)["state"] != "active":
                        raise ValueError("Meeting creation was cancelled")
                    return bot_id

                bot_id = await asyncio.to_thread(provision)
            return JSONResponse({"session_id": request_id, "bot_id": bot_id}, status_code=201)
        except (ValueError, TypeError):
            return JSONResponse({"error": "invalid_or_conflicting_request"}, status_code=400)
        except Exception:
            counters["creation_failures_total"] += 1
            return JSONResponse({"error": "creation_not_confirmed"}, status_code=503)

    async def connect(websocket: WebSocket, channel):
        nonlocal handshakes
        session_id = websocket.query_params.get("session", "")
        supplied = websocket.query_params.get("token", "")
        row = await store_call(store.authorize, session_id, supplied)
        if not accepting or not row or handshakes >= max_sessions * 2:
            counters["connections_rejected_total"] += 1
            await websocket.close(code=1008)
            return
        handshakes += 1
        session = None
        registered = False
        try:
            await websocket.accept()
            raw = await asyncio.wait_for(websocket.receive(), 10)
            text = raw.get("text")
            if not text or len(text) > 4096:
                raise ValueError("Invalid handshake")
            bot_id = ready_bot_id(json.loads(text))
            # create_bot can connect its sockets before the HTTP response returns.
            deadline = time.monotonic() + 10
            while row["bot_id"] is None and time.monotonic() < deadline:
                await asyncio.sleep(0.1)
                row = await store_call(store.authorize, session_id, supplied)
                if not row:
                    raise ValueError("Registration was revoked")
            if row["state"] != "active" or row["bot_id"] != bot_id:
                raise ValueError("Bot does not match registered credentials")
            async with admission_lock:
                session = sessions.get(bot_id)
                if session is None:
                    if len(sessions) >= max_sessions:
                        await close(websocket, 1013)
                        return
                    if not await store_call(store.claim, session_id, owner):
                        raise ValueError("Meeting lease is unavailable")
                    session = Session(session_id, bot_id, row["expires"])
                    sessions[bot_id] = session
                    session.task = asyncio.create_task(run_session(session))
                if (
                    session.session_id != session_id
                    or channel in session.sockets
                    or session.disconnected.is_set()
                ):
                    raise ValueError("Duplicate or mismatched connection")
                session.sockets[channel] = websocket
                registered = True
                if len(session.sockets) == 2:
                    session.paired.set()
            controls = deque(maxlen=21)
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
                data, text = message.get("bytes"), message.get("text")
                if data is not None:
                    if channel != "audio" or len(data) > MAX_AUDIO_BYTES + 8202:
                        raise ValueError("Invalid binary message")
                    await asyncio.wait_for(session.started.wait(), 30)
                    await session.transport.input().receive_audio(data)
                elif text is not None:
                    controls.append(time.monotonic())
                    if len(text) > 16384 or (
                        len(controls) == 21 and controls[-1] - controls[0] < 1
                    ):
                        raise ValueError("Control limit exceeded")
                    event = json.loads(text)
                    if (
                        not isinstance(event, dict)
                        or event.get("bot_id", bot_id) != bot_id
                        or event.get("type") == "PCMChunk"
                    ):
                        raise ValueError("Invalid control message")
                    if event.get("status") == "failed" or event.get("success") is False:
                        counters["meeting_command_failures_total"] += 1
                        session.reason = "meeting_command_failed"
                        await close(websocket, 1011)
                        break
                    if (
                        channel == "control"
                        and event.get("command") == "resume_audio_ack"
                        and session.transport
                    ):
                        session.transport.output().acknowledge_resume(event)
                    if (
                        channel == "control"
                        and event.get("command") == "interrupt"
                        and "status" not in event
                    ):
                        await asyncio.wait_for(session.started.wait(), 30)
                        await asyncio.wait_for(session.transport.input().ready.wait(), 10)
                        await session.transport.input().broadcast_interruption()
        except AudioOverload:
            counters["audio_overload_total"] += 1
            if session:
                session.reason = "audio_overload"
            await close(websocket, 1013)
        except (ValueError, TypeError, TimeoutError, KeyError):
            counters["protocol_errors_total"] += 1
            await close(websocket, 1008)
        except WebSocketDisconnect:
            pass
        except Exception:
            counters["connection_errors_total"] += 1
            await close(websocket, 1011)
        finally:
            handshakes -= 1
            if session and registered:
                session.disconnected.set()

    @app.websocket("/bridge/audio")
    async def audio(websocket: WebSocket):
        await connect(websocket, "audio")

    @app.websocket("/bridge")
    async def control(websocket: WebSocket):
        await connect(websocket, "control")

    return app
