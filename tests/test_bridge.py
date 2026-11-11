"""Real Pipecat transports + a synthetic meeting; no paid provider requests."""

import asyncio
import base64
import json
import os
import socket
import tempfile
import time
import unittest
from unittest.mock import patch

import uvicorn
from loguru import logger
from pipecat.frames.frames import (
    ErrorFrame,
    InputAudioRawFrame,
    InterruptionFrame,
    TTSAudioRawFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from meetstream_pipecat.__main__ import bot_payload
from meetstream_pipecat.protocol import (
    MAX_AUDIO_BYTES,
    audio_command,
    decode_audio,
    ready_bot_id,
)
from meetstream_pipecat.server import create_app
from meetstream_pipecat.store import Store

logger.remove()


def packet(pcm=b"\x10\x00" * 9600, speaker_id="human", speaker_name="Alice"):
    data = bytearray(b"\x01")
    for name in (speaker_id, speaker_name):
        encoded = name.encode()
        data.extend(len(encoded).to_bytes(2, "little"))
        data.extend(encoded)
    return bytes(data) + pcm


class ProtocolTests(unittest.TestCase):
    def test_live_google_meet_format_bearing_24khz_audio(self):
        data = b"\x04" + (24000).to_bytes(4, "little") + b"\x01" + packet()[1:]
        frame = decode_audio(data)
        self.assertEqual(frame.sample_rate, 24000)
        self.assertEqual(frame.speaker_name, "Alice")
        self.assertEqual(len(frame.pcm), 19200)
        for bad in (data[:4], data[:5] + b"\x02" + data[6:], b"\x04" + bytes(4) + data[5:]):
            with self.assertRaises(ValueError):
                decode_audio(bad)

    def test_wire_format_and_reject_bad_frames(self):
        audio = decode_audio(packet(speaker_name="佐藤太郎"))
        self.assertEqual(audio.speaker_name, "佐藤太郎")
        self.assertEqual(audio.pcm, b"\x10\x00" * 9600)
        for invalid in (
            b"",
            b"\x02",
            b"\x01",
            b"\x01\xff\xffx",
            b"\x01\x00\x00\x03\x00x",
            packet(b"x"),
            packet(b"x" * (MAX_AUDIO_BYTES + 2)),
            b"\x01\x01\x00\xff\x00\x00",
        ):
            with self.subTest(invalid=invalid[:20]), self.assertRaises(ValueError):
                decode_audio(invalid)
        with self.assertRaises(ValueError):
            ready_bot_id({"type": "ready", "bot_id": ""})
        command = audio_command("bot", b"\x00\x01")
        self.assertEqual(base64.b64decode(command["audiochunk"]), b"\x00\x01")
        self.assertEqual(command["sample_rate"], 48000)

    def test_bot_payload_uses_two_authenticated_wss_channels(self):
        with patch.dict(
            os.environ,
            {
                "PUBLIC_URL": "https://bridge.example",
                "BRIDGE_TOKEN": "secret" * 6,
                "DEPLOYMENT_ID": "a" * 32,
            },
        ):
            data = bot_payload(
                "https://meet.google.com/example",
                session_id="fixture-session",
                session_token="fixture-token",
            )
        self.assertFalse(data["video_required"])
        self.assertIn(
            "wss://bridge.example/bridge/audio?session=fixture-session&token=fixture-token",
            data["live_audio_required"]["websocket_url"],
        )
        self.assertIn(
            "wss://bridge.example/bridge?session=fixture-session&token=fixture-token",
            data["socket_connection_url"]["websocket_url"],
        )
        with self.assertRaises(ValueError):
            create_app("short")


class SyntheticVoice(FrameProcessor):
    """Return one second of 24 kHz speech; reset on a real interruption frame."""

    def __init__(self, meeting_input):
        super().__init__()
        self.meeting_input = meeting_input
        self.respond = True
        self.received_rates = []
        self.interruptions = 0

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            self.respond = True
            self.interruptions += 1
            await self.push_frame(frame, direction)
        elif isinstance(frame, InputAudioRawFrame):
            await self.meeting_input.audio_consumed(frame)
            self.received_rates.append(frame.sample_rate)
            if self.respond:
                self.respond = False
                await self.push_frame(
                    TTSAudioRawFrame(
                        b"\x10\x00" * 24000, 24000, 1, context_id=getattr(self, "context_id", None)
                    )
                )
        else:
            await self.push_frame(frame, direction)


class LateTTSVoice(SyntheticVoice):
    context_id = "old-response"

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            self.context_id = "new-response"

            async def late_audio():
                await asyncio.sleep(0.1)
                await self.push_frame(
                    TTSAudioRawFrame(b"\x20\x00" * 24000, 24000, 1, context_id="old-response")
                )

            asyncio.create_task(late_audio())


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.voices = []
        self.audio_send_times = []

        def factory(transport):
            original_send = transport.output().websocket.send_json

            async def observe_send(data):
                if data.get("command") == "sendaudio":
                    self.audio_send_times.append(time.monotonic())
                await original_send(data)

            transport.output().websocket.send_json = observe_send
            voice = getattr(self, "voice_type", SyntheticVoice)(transport.input())
            self.voices.append(voice)
            return PipelineWorker(
                Pipeline([transport.input(), voice, transport.output()]),
                params=PipelineParams(audio_in_sample_rate=16000, audio_out_sample_rate=48000),
                idle_timeout_secs=None,
            )

        self.directory = tempfile.TemporaryDirectory()
        self.store = Store(self.directory.name)
        self.session_id, self.token = self.store.reserve("https://meet.google.com/fixture")
        self.store.bind(self.session_id, "bot-test")
        self.admin = "test-administrator-secret"
        self.environment = patch.dict(os.environ, {"EXTERNAL_REAPER": "true"})
        self.environment.start()
        self.store.set_meta("reaper_heartbeat", time.time())
        self.factory = factory
        self.app = create_app(self.admin, agent_factory=factory, store=self.store)
        self.socket = socket.socket()
        self.socket.bind(("127.0.0.1", 0))
        self.base = f"ws://127.0.0.1:{self.socket.getsockname()[1]}"
        self.server = uvicorn.Server(
            uvicorn.Config(self.app, log_level="critical", access_log=False)
        )
        self.serving = asyncio.create_task(self.server.serve(sockets=[self.socket]))
        for _ in range(200):
            if self.server.started:
                return
            await asyncio.sleep(0.01)
        self.fail("Test server did not start")

    async def asyncTearDown(self):
        self.server.should_exit = True
        await asyncio.wait_for(self.serving, timeout=15)
        self.socket.close()
        self.assertEqual(self.app.state.sessions, {})
        self.environment.stop()
        self.directory.cleanup()

    def url(self, channel):
        path = "/bridge/audio" if channel == "audio" else "/bridge"
        return f"{self.base}{path}?session={self.session_id}&token={self.token}"

    async def receive(self, ws):
        return json.loads(await asyncio.wait_for(ws.recv(), timeout=4))

    async def ready(self, ws, bot_id="bot-test"):
        await ws.send(json.dumps({"type": "ready", "bot_id": bot_id}))

    async def test_auth_and_handshake_rejection(self):
        with self.assertRaises(InvalidStatus):
            async with connect(f"{self.base}/bridge?token=wrong"):
                self.fail("Invalid token accepted")
        async with connect(self.url("control")) as ws:
            await ws.send(json.dumps({"type": "ready", "bot_id": ""}))
            with self.assertRaises(ConnectionClosed) as closed:
                await ws.recv()
            self.assertEqual(closed.exception.rcvd.code, 1008)

    async def test_audio_first_pairing_pacing_interrupt_resume_and_cleanup(self):
        async with connect(self.url("audio")) as audio:
            await self.ready(audio)
            # Audio is allowed to arrive before the control socket.
            await audio.send(b"\x04" + (24000).to_bytes(4, "little") + b"\x01" + packet()[1:])
            async with connect(self.url("control")) as control:
                await self.ready(control)
                self.assertEqual((await self.receive(control))["command"], "resume_audio")
                first = await self.receive(control)
                self.assertEqual(first["command"], "sendaudio")
                self.assertEqual(first["sample_rate"], 48000)
                self.assertEqual(len(base64.b64decode(first["audiochunk"])), 9600)
                self.assertEqual((await self.receive(control))["command"], "sendaudio")
                self.assertGreater(self.audio_send_times[1] - self.audio_send_times[0], 0.09)
                await control.send(json.dumps({"command": "interrupt", "bot_id": "bot-test"}))
                for _ in range(12):
                    command = await self.receive(control)
                    if command["command"] == "interrupt":
                        break
                else:
                    self.fail("Remote queue never interrupted")
                self.assertEqual(command["action"], "clear_audio_queue")
                await control.send(json.dumps({"command": "interrupt", "status": "cleared"}))
                # The ack must not create a second interrupt; silence until new speech.
                with self.assertRaises(TimeoutError):
                    await asyncio.wait_for(control.recv(), timeout=0.15)
                await audio.send(packet(speaker_name="MeetStream Pipecat"))
                with self.assertRaises(TimeoutError):
                    await asyncio.wait_for(control.recv(), timeout=0.15)
                await audio.send(packet())
                self.assertEqual((await self.receive(control))["command"], "resume_audio")
                self.assertEqual((await self.receive(control))["command"], "sendaudio")
                self.assertEqual(set(self.voices[0].received_rates), {16000})
                self.assertEqual(self.voices[0].interruptions, 1)
                await audio.close()
                with self.assertRaises(ConnectionClosed):
                    while True:
                        await asyncio.wait_for(control.recv(), timeout=4)

    async def test_control_first_duplicate_and_malformed_audio(self):
        async with connect(self.url("control")) as control:
            await self.ready(control)
            async with connect(self.url("control")) as duplicate:
                await self.ready(duplicate)
                with self.assertRaises(ConnectionClosed) as closed:
                    await asyncio.wait_for(duplicate.recv(), timeout=4)
                self.assertEqual(closed.exception.rcvd.code, 1008)
            async with connect(self.url("audio")) as audio:
                await self.ready(audio)
                await audio.send(b"\x01\xff\xffx")
                with self.assertRaises(ConnectionClosed) as closed:
                    await asyncio.wait_for(audio.recv(), timeout=4)
                self.assertEqual(closed.exception.rcvd.code, 1008)
                with self.assertRaises(ConnectionClosed):
                    await asyncio.wait_for(control.recv(), timeout=4)

    async def test_late_cancelled_tts_audio_cannot_restart_playback(self):
        self.voice_type = LateTTSVoice
        async with connect(self.url("audio")) as audio, connect(self.url("control")) as control:
            await self.ready(audio)
            await self.ready(control)
            await audio.send(packet())
            self.assertEqual((await self.receive(control))["command"], "resume_audio")
            self.assertEqual((await self.receive(control))["command"], "sendaudio")
            await control.send(json.dumps({"command": "interrupt", "bot_id": "bot-test"}))
            while (await self.receive(control))["command"] != "interrupt":
                pass
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(control.recv(), 0.3)
            await audio.send(packet())
            self.assertEqual((await self.receive(control))["command"], "resume_audio")
            self.assertEqual((await self.receive(control))["command"], "sendaudio")

    def zoom_registration(self):
        with self.store.connection() as db:
            db.execute(
                "UPDATE meetings SET meeting_url=? WHERE id=?",
                ("https://zoom.us/j/123", self.session_id),
            )

    async def test_zoom_waits_for_matching_unmute_ack_before_audio(self):
        self.zoom_registration()
        async with connect(self.url("audio")) as audio, connect(self.url("control")) as control:
            await self.ready(audio)
            await self.ready(control)
            await audio.send(packet())
            resume = await self.receive(control)
            self.assertEqual(resume["command"], "resume_audio")
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(control.recv(), 0.1)
            await control.send(
                json.dumps({"command": "resume_audio_ack", "resume_id": "stale", "success": True})
            )
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(control.recv(), 0.1)
            await control.send(
                json.dumps(
                    {
                        "command": "resume_audio_ack",
                        "resume_id": resume["resume_id"],
                        "success": True,
                    }
                )
            )
            self.assertEqual((await self.receive(control))["command"], "sendaudio")

    async def test_zoom_missing_unmute_ack_ends_both_channels(self):
        self.zoom_registration()
        async with connect(self.url("audio")) as audio, connect(self.url("control")) as control:
            await self.ready(audio)
            await self.ready(control)
            await audio.send(packet())
            self.assertEqual((await self.receive(control))["command"], "resume_audio")
            with self.assertRaises(ConnectionClosed):
                await asyncio.wait_for(control.recv(), 7)
            with self.assertRaises(ConnectionClosed):
                await asyncio.wait_for(audio.recv(), 2)
            self.assertEqual(self.app.state.counters["meeting_command_failures_total"], 1)

    async def test_command_failure_ends_both_channels(self):
        async with connect(self.url("audio")) as audio, connect(self.url("control")) as control:
            await self.ready(audio)
            await self.ready(control)
            await control.send(
                json.dumps(
                    {
                        "command": "resume_audio_ack",
                        "success": False,
                        "error": "zoom_self_unmute_failed",
                    }
                )
            )
            with self.assertRaises(ConnectionClosed) as closed:
                await asyncio.wait_for(control.recv(), timeout=4)
            self.assertEqual(closed.exception.rcvd.code, 1011)
            with self.assertRaises(ConnectionClosed):
                await asyncio.wait_for(audio.recv(), timeout=4)

    async def test_cross_tenant_tokens_and_bot_spoofing_are_rejected(self):
        sid, token = self.store.reserve("https://meet.google.com/other", "tenant-b")
        self.store.bind(sid, "other-bot")
        with self.assertRaises(InvalidStatus):
            async with connect(f"{self.base}/bridge?session={sid}&token={self.token}"):
                self.fail("Cross-session credential accepted")
        with self.assertRaises(InvalidStatus):
            async with connect(f"{self.base}/bridge?session={self.session_id}&token={self.admin}"):
                self.fail("Administrator token accepted as a meeting credential")
        async with connect(self.url("control")) as ws:
            await self.ready(ws, "other-bot")
            with self.assertRaises(ConnectionClosed) as closed:
                await asyncio.wait_for(ws.recv(), 2)
            self.assertEqual(closed.exception.rcvd.code, 1008)
        self.assertEqual(self.voices, [])
        self.assertEqual(self.store.get(sid)["state"], "active")
        self.assertEqual(self.store.get(self.session_id)["state"], "active")

    async def test_revoked_credentials_cannot_restart_a_pipeline(self):
        self.store.stopping(self.session_id, "operator_stop")
        with self.assertRaises(InvalidStatus):
            async with connect(self.url("audio")):
                self.fail("Revoked credential accepted")
        self.assertEqual(self.voices, [])

    async def test_creation_response_race_waits_for_registered_bot_binding(self):
        sid, token = self.store.reserve("https://meet.google.com/pending")
        async with connect(f"{self.base}/bridge?session={sid}&token={token}") as ws:
            await self.ready(ws, "pending-bot")
            await asyncio.sleep(0.05)
            self.assertEqual(self.voices, [])
            self.store.bind(sid, "pending-bot")
            async with connect(f"{self.base}/bridge/audio?session={sid}&token={token}") as audio:
                await self.ready(audio, "pending-bot")
                await audio.send(packet())
                self.assertEqual((await self.receive(ws))["command"], "resume_audio")
                self.assertEqual((await self.receive(ws))["command"], "sendaudio")

    async def test_deadline_closes_and_revokes_both_channels(self):
        with self.store.connection() as db:
            db.execute(
                "UPDATE meetings SET expires=? WHERE id=?", (time.time() + 0.3, self.session_id)
            )
        async with connect(self.url("control")) as control, connect(self.url("audio")) as audio:
            await self.ready(control)
            await self.ready(audio)
            with self.assertRaises(ConnectionClosed):
                await asyncio.wait_for(control.recv(), 3)
            with self.assertRaises(ConnectionClosed):
                await asyncio.wait_for(audio.recv(), 3)
        self.assertEqual(self.store.get(self.session_id)["state"], "stopping")
        self.assertIsNone(self.store.authorize(self.session_id, self.token))

    async def test_slow_audio_consumer_is_disconnected_with_bounded_memory(self):
        class NoAck(SyntheticVoice):
            async def process_frame(self, frame, direction):
                await FrameProcessor.process_frame(self, frame, direction)
                if not isinstance(frame, InputAudioRawFrame):
                    await self.push_frame(frame, direction)

        self.voice_type = NoAck
        async with connect(self.url("control")) as control, connect(self.url("audio")) as audio:
            await self.ready(control)
            await self.ready(audio)
            await audio.send(packet(pcm=b"\x10\x00" * 144000))
            with self.assertRaises(ConnectionClosed):
                await asyncio.wait_for(audio.recv(), 5)
        self.assertLessEqual(self.voices[0].meeting_input.window.high_water, 1)
        self.assertLessEqual(len(self.voices[0].meeting_input.pending_frames), 50)
        self.assertEqual(self.store.get(self.session_id)["state"], "stopping")

    async def test_provider_failure_cancels_speech_and_sends_safe_fallback(self):
        class BrokenProvider(SyntheticVoice):
            async def process_frame(self, frame, direction):
                await FrameProcessor.process_frame(self, frame, direction)
                if isinstance(frame, InputAudioRawFrame):
                    await self.meeting_input.audio_consumed(frame)
                    if self.respond:
                        self.respond = False
                        await self.push_frame(
                            ErrorFrame(error="private upstream failure", fatal=True),
                            FrameDirection.UPSTREAM,
                        )
                else:
                    await self.push_frame(frame, direction)

        self.voice_type = BrokenProvider
        async with connect(self.url("control")) as control, connect(self.url("audio")) as audio:
            await self.ready(control)
            await self.ready(audio)
            await audio.send(packet())
            messages = []
            with self.assertRaises(ConnectionClosed):
                while True:
                    messages.append(await self.receive(control))
        self.assertIn("interrupt", [m["command"] for m in messages])
        notice = next(m for m in messages if m["command"] == "sendmsg")
        self.assertIn("try again", notice["message"])
        self.assertNotIn("private", json.dumps(messages))
        self.assertGreater(self.app.state.counters["pipeline_errors_total"], 0)

    async def test_metrics_require_admin_and_reveal_no_meeting_identifiers(self):
        import httpx

        async with httpx.AsyncClient(base_url=self.base.replace("ws:", "http:")) as client:
            self.assertEqual((await client.get("/metrics")).status_code, 401)
            response = await client.get(
                "/metrics", headers={"Authorization": "Bearer " + self.admin}
            )
            self.assertEqual(response.status_code, 200)
            self.assertIn("meetstream_active_sessions 0", response.text)
            self.assertNotIn(self.token, response.text)
            self.assertNotIn(self.session_id, response.text)
            self.assertEqual((await client.get("/ready")).status_code, 200)

    async def test_simultaneous_socket_admission_creates_one_pipeline(self):
        async with connect(self.url("control")) as control, connect(self.url("audio")) as audio:
            await asyncio.gather(self.ready(control), self.ready(audio))
            await audio.send(packet())
            self.assertEqual((await self.receive(control))["command"], "resume_audio")
            self.assertEqual((await self.receive(control))["command"], "sendaudio")
            self.assertEqual(len(self.voices), 1)
            self.assertEqual(len(self.app.state.sessions), 1)

    async def test_admin_creation_is_idempotent_and_hides_transport_credentials(self):
        from urllib.parse import parse_qs, urlsplit
        from uuid import uuid4

        import httpx

        key = uuid4().hex
        creates = []

        def api(request):
            creates.append(request)
            return httpx.Response(201, json={"bot_id": "admin-created-bot"})

        def client():
            return httpx.Client(base_url="https://api.example/", transport=httpx.MockTransport(api))

        headers = {"Authorization": "Bearer " + self.admin, "Idempotency-Key": key}
        body = {
            "meeting_url": "https://meet.google.com/created",
            "tenant": "tenant-b",
            "max_seconds": 120,
        }
        with (
            patch.dict(
                os.environ, {"PUBLIC_URL": "https://bridge.example", "DEPLOYMENT_ID": "a" * 32}
            ),
            patch("meetstream_pipecat.server.api_client", side_effect=client),
        ):
            async with httpx.AsyncClient(base_url=self.base.replace("ws:", "http:")) as c:
                self.assertEqual((await c.post("/meetings", json=body)).status_code, 401)
                first = await c.post("/meetings", headers=headers, json=body)
                self.assertEqual(first.status_code, 201)
                self.assertEqual(
                    (await c.post("/meetings", headers=headers, json=body)).status_code, 201
                )
                body["meeting_url"] = "https://meet.google.com/different"
                self.assertEqual(
                    (await c.post("/meetings", headers=headers, json=body)).status_code, 400
                )
        self.assertEqual(len(creates), 1)
        self.assertEqual(creates[0].headers["Idempotency-Key"], key)
        payload = json.loads(creates[0].content)
        query = parse_qs(urlsplit(payload["live_audio_required"]["websocket_url"]).query)
        secret = query["token"][0]
        self.assertNotEqual(secret, self.admin)
        self.assertNotIn(secret, first.text)
        self.assertEqual(query["session"], [key])
        self.assertEqual(self.store.authorize(key, secret)["bot_id"], "admin-created-bot")
        self.assertFalse(payload["audio_required"])


if __name__ == "__main__":
    unittest.main()
