"""MeetStream wire protocol. No Pipecat dependency; reusable by a LiveKit bridge."""

import base64
from dataclasses import dataclass

SAMPLE_RATE = 48_000
MAX_AUDIO_BYTES = SAMPLE_RATE * 2 * 3  # At most three seconds per incoming message.


@dataclass(frozen=True)
class MeetingAudio:
    speaker_id: str
    speaker_name: str
    pcm: bytes
    sample_rate: int = SAMPLE_RATE


def decode_audio(data: bytes) -> MeetingAudio:
    """Read legacy 0x01 or format-bearing 0x04 PCM16-LE frames."""
    if not data or data[0] not in (1, 4):
        raise ValueError("Expected a type 0x01 or 0x04 audio frame")
    offset, names, sample_rate = 1, [], SAMPLE_RATE
    if data[0] == 4:
        if len(data) < 6:
            raise ValueError("Truncated audio format")
        sample_rate = int.from_bytes(data[1:5], "little")
        if sample_rate not in (8000, 16000, 24000, 32000, 44100, 48000) or data[5] != 1:
            raise ValueError("Unsupported audio rate or channel count")
        offset = 6
    for _ in range(2):
        if offset + 2 > len(data):
            raise ValueError("Truncated speaker length")
        size = int.from_bytes(data[offset : offset + 2], "little")
        offset += 2
        if size > 4096 or offset + size > len(data):
            raise ValueError("Invalid speaker metadata length")
        names.append(data[offset : offset + size].decode("utf-8"))
        offset += size
    pcm = data[offset:]
    if len(pcm) % 2 or len(pcm) > sample_rate * 2 * 3:
        raise ValueError("Invalid PCM16 length")
    return MeetingAudio(*names, pcm, sample_rate)


def audio_command(bot_id: str, pcm: bytes) -> dict:
    if not pcm or len(pcm) % 2 or len(pcm) > MAX_AUDIO_BYTES:
        raise ValueError("Expected nonempty, aligned PCM16")
    return {
        "command": "sendaudio",
        "bot_id": bot_id,
        "audiochunk": base64.b64encode(pcm).decode("ascii"),
        "sample_rate": SAMPLE_RATE,
        "encoding": "pcm16",
        "channels": 1,
        "endianness": "little",
    }


def interrupt_command(bot_id: str) -> dict:
    return {"command": "interrupt", "bot_id": bot_id, "action": "clear_audio_queue"}


def resume_command(bot_id: str, resume_id: str) -> dict:
    return {"command": "resume_audio", "bot_id": bot_id, "resume_id": resume_id}


def ready_bot_id(message: dict) -> str:
    bot_id = message.get("bot_id")
    if message.get("type") != "ready" or not isinstance(bot_id, str) or not bot_id.strip():
        raise ValueError("Expected ready handshake with bot_id")
    if len(bot_id) > 200 or not bot_id.isprintable():
        raise ValueError("Invalid bot_id")
    return bot_id
