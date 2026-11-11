"""Bound audio still in flight, rather than only bounding the WebSocket queue."""

import asyncio
import time

from pipecat.frames.frames import InputAudioRawFrame, LLMContextFrame
from pipecat.processors.frame_processor import FrameProcessor


class AudioOverload(ValueError):
    pass


class AudioWindow:
    def __init__(self, seconds, timeout=2):
        self.limit, self.timeout = seconds, timeout
        self.pending = 0.0
        self.high_water = 0.0
        self.generation = 0
        self.changed = asyncio.Condition()

    async def reserve(self, duration):
        generation = self.generation
        if duration > self.limit:
            raise AudioOverload("Audio packet exceeds backlog budget")
        try:
            async with asyncio.timeout(self.timeout), self.changed:
                await self.changed.wait_for(
                    lambda: self.pending + duration <= self.limit or generation != self.generation
                )
                if generation != self.generation:
                    return False
                self.pending += duration
                self.high_water = max(self.high_water, self.pending)
            return True
        except TimeoutError as exc:
            raise AudioOverload("Audio consumer is not keeping up") from exc

    async def release(self, duration, generation=None):
        async with self.changed:
            if generation is None or generation == self.generation:
                self.pending = max(0, self.pending - duration)
            self.changed.notify_all()

    async def clear(self):
        async with self.changed:
            self.generation += 1
            self.pending = 0
            self.changed.notify_all()


class AudioDrain(FrameProcessor):
    """After STT/VAD have consumed raw audio, release its ingress credit and drop it."""

    def __init__(self, meeting_input):
        super().__init__()
        self.meeting_input = meeting_input

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame):
            await self.meeting_input.audio_consumed(frame)
        else:
            await self.push_frame(frame, direction)


class AudioClock:
    def __init__(self):
        self.started = time.monotonic()
        self.received = 0

    def accept(self, seconds):
        self.received += seconds
        if self.received > (time.monotonic() - self.started) * 1.25 + 3:
            raise AudioOverload("Audio arrived faster than its playback clock")


class ContextWindow(FrameProcessor):
    """Bound plain text history; the system instruction lives on the LLM service."""

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, LLMContextFrame) and len(frame.context.messages) > 40:
            frame.context.set_messages(frame.context.messages[-40:])
        await self.push_frame(frame, direction)
