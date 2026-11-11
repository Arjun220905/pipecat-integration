"""Pipecat processors for MeetStream's separate audio and control sockets."""

import asyncio
import logging
from uuid import uuid4

from fastapi import WebSocket
from pipecat.audio.resamplers.soxr_stream_resampler import SOXRStreamAudioResampler
from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    StartFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.transports.base_transport import BaseTransport, TransportParams

from .flow import AudioClock, AudioDrain, AudioOverload, AudioWindow
from .protocol import SAMPLE_RATE, audio_command, decode_audio, interrupt_command, resume_command

logger = logging.getLogger(__name__)


class MeetingInput(BaseInputTransport):
    def __init__(self, params: TransportParams, bot_name: str):
        super().__init__(params)
        self.bot_name = bot_name
        self.ready = asyncio.Event()
        self.resampler = SOXRStreamAudioResampler(clear_after_secs=None)
        self.source_rate = None
        self.window = AudioWindow(1)
        self.pending_frames = {}
        self.clock = AudioClock()
        self.on_overload = None

    async def audio_consumed(self, frame):
        duration = self.pending_frames.pop(frame.id, 0)
        await self.window.release(duration)

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self.set_transport_ready(frame)
        self.ready.set()

    async def receive_audio(self, data: bytes):
        audio = decode_audio(data)
        self.clock.accept(len(audio.pcm) / (audio.sample_rate * 2))
        # Attribution describes the dominant speaker of a MIXED stream. This only
        # reduces obvious self-echo; it cannot isolate a human during overlap.
        if not audio.pcm or audio.speaker_name == self.bot_name:
            return
        await asyncio.wait_for(self.ready.wait(), timeout=30)
        if self.source_rate != audio.sample_rate:
            self.resampler = SOXRStreamAudioResampler(clear_after_secs=None)
            self.source_rate = audio.sample_rate
        pcm = await self.resampler.resample(audio.pcm, audio.sample_rate, self.sample_rate)
        # Credits are returned only AFTER STT and VAD, not when an internal
        # unbounded Pipecat queue merely accepts the frame.
        size = self.sample_rate // 50 * 2  # 20 ms, PCM16 mono.
        for offset in range(0, len(pcm), size):
            frame = InputAudioRawFrame(pcm[offset : offset + size], self.sample_rate, 1)
            duration = len(frame.audio) / (self.sample_rate * 2)
            try:
                await self.window.reserve(duration)
            except AudioOverload:
                if self.on_overload:
                    self.on_overload()
                raise
            self.pending_frames[frame.id] = duration
            await self.push_audio_frame(frame)


class MeetingOutput(BaseOutputTransport):
    def __init__(
        self,
        params: TransportParams,
        websocket: WebSocket,
        bot_id: str,
        *,
        require_resume_ack=False,
    ):
        super().__init__(params)
        self.websocket, self.bot_id = websocket, bot_id
        self.lock = asyncio.Lock()
        self.needs_resume = True
        self.require_resume_ack = require_resume_ack
        self.resume_id = None
        self.resumed = asyncio.Event()
        self.on_command_failure = None
        self.audio_contexts = set()
        self.interrupted_contexts = set()
        self.blocked = False
        self.window = AudioWindow(5)
        self.on_overload = None

    def acknowledge_resume(self, event):
        if event.get("resume_id") == self.resume_id and event.get("success") is True:
            self.resumed.set()

    async def queue_frame(self, frame, direction=FrameDirection.DOWNSTREAM, callback=None):
        context_id = getattr(frame, "context_id", None)
        if context_id:
            if context_id in self.interrupted_contexts:
                return
            self.audio_contexts.add(context_id)
        if isinstance(frame, OutputAudioRawFrame):
            duration = len(frame.audio) / (frame.sample_rate * frame.num_channels * 2)
            try:
                if not await self.window.reserve(duration):
                    return
            except AudioOverload:
                if self.on_overload:
                    self.on_overload()
                raise
        await super().queue_frame(frame, direction, callback)

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self.set_transport_ready(frame)

    async def notice(self, message):
        async with asyncio.timeout(2), self.lock:
            await self.websocket.send_json(
                {
                    "command": "sendmsg",
                    "bot_id": self.bot_id,
                    "message": message,
                    "msg": message,
                }
            )

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        if isinstance(frame, InterruptionFrame):
            # Retain cancelled IDs for this bounded meeting: providers can deliver
            # late audio even after Pipecat's local queues have been cleared.
            self.interrupted_contexts.update(self.audio_contexts)
            self.audio_contexts.clear()
            self.blocked = True
            await self.window.clear()
            # Pipecat cancels its local output task and discards queued old audio.
            await super().process_frame(frame, direction)
            async with self.lock:
                await self.websocket.send_json(interrupt_command(self.bot_id))
                self.needs_resume = True
            self.blocked = False
            logger.info("Playback interrupted for bot %s", self.bot_id)
        else:
            await super().process_frame(frame, direction)

    async def write_audio_frame(self, frame: OutputAudioRawFrame) -> bool:
        generation = self.window.generation
        if self.blocked:
            return False
        if frame.sample_rate != SAMPLE_RATE or frame.num_channels != 1:
            raise ValueError("Meeting output must be 48 kHz mono")
        async with self.lock:
            if self.blocked:
                return False
            if self.needs_resume:
                self.resume_id = str(uuid4())
                self.resumed.clear()
                await self.websocket.send_json(resume_command(self.bot_id, self.resume_id))
                if self.require_resume_ack:
                    try:
                        await asyncio.wait_for(self.resumed.wait(), 5)
                    except TimeoutError:
                        if self.on_command_failure:
                            self.on_command_failure()
                        raise
                self.needs_resume = False
            if self.blocked or generation != self.window.generation:
                return False
            await self.websocket.send_json(audio_command(self.bot_id, frame.audio))
        # Pace playback in real time. Never hold the control lock while sleeping:
        # an interruption must bypass this wait and clear the remote queue.
        await asyncio.sleep(len(frame.audio) / (SAMPLE_RATE * 2))
        await self.window.release(len(frame.audio) / (SAMPLE_RATE * 2), generation)
        return True


class MeetStreamTransport(BaseTransport):
    def __init__(
        self,
        control: WebSocket,
        bot_id: str,
        bot_name: str = "MeetStream Pipecat",
        *,
        require_resume_ack=False,
    ):
        super().__init__()
        params = TransportParams(
            audio_in_enabled=True,
            audio_in_sample_rate=16000,
            audio_out_enabled=True,
            audio_out_sample_rate=SAMPLE_RATE,
            audio_out_10ms_chunks=10,
            audio_out_auto_silence=False,
            audio_out_end_silence_secs=0,
        )
        self._input = MeetingInput(params, bot_name)
        self._output = MeetingOutput(params, control, bot_id, require_resume_ack=require_resume_ack)

    def input(self) -> MeetingInput:
        return self._input

    def output(self) -> MeetingOutput:
        return self._output

    def drain(self):
        return AudioDrain(self._input)
