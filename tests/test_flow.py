import asyncio
import importlib.util
import io
import os
import unittest
from pathlib import Path

from meetstream_pipecat.flow import AudioClock, AudioOverload, AudioWindow


class AudioBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_consumer_cannot_exceed_ingress_budget(self):
        window = AudioWindow(1, timeout=0.01)
        accepted = 0
        with self.assertRaises(AudioOverload):
            for _ in range(1000):
                await window.reserve(0.02)
                accepted += 1
        self.assertLessEqual(accepted, 50)
        self.assertLessEqual(window.high_water, 1)
        self.assertLessEqual(window.pending, 1)

    async def test_interrupt_wakes_producer_and_discards_old_audio(self):
        window = AudioWindow(5)
        await window.reserve(5)
        generation = window.generation
        blocked = asyncio.create_task(window.reserve(0.1))
        await asyncio.sleep(0)
        self.assertFalse(blocked.done())
        await window.clear()
        self.assertFalse(await blocked)
        self.assertTrue(await window.reserve(0.2))
        await window.release(5, generation)
        self.assertAlmostEqual(window.pending, 0.2)
        await window.release(0.2, window.generation)
        self.assertEqual(window.pending, 0)

    async def test_oversized_output_and_unrealistic_input_rate_are_rejected(self):
        with self.assertRaises(AudioOverload):
            await AudioWindow(5).reserve(6)
        clock = AudioClock()
        clock.accept(3)
        with self.assertRaises(AudioOverload):
            clock.accept(1)


@unittest.skipUnless(os.getenv("CONTAINERIZED") == "1", "production image check")
class NativeAudioTests(unittest.TestCase):
    def test_soundfile_loads_patched_library_instead_of_bundled_binary(self):
        import soundfile

        self.assertIsNone(importlib.util.find_spec("_soundfile_data"))
        self.assertIn("/usr/local/lib/libsndfile.so", Path("/proc/self/maps").read_text())
        self.assertIn(
            "c32c090ff9bb45e1dae637957c9c9b47b3967623",
            Path("/usr/local/share/libsndfile/SOURCE").read_text(),
        )
        stream = io.BytesIO()
        soundfile.write(stream, [0.0, 0.25, -0.25], 48000, format="WAV", subtype="PCM_16")
        stream.seek(0)
        audio, rate = soundfile.read(stream)
        self.assertEqual(rate, 48000)
        self.assertEqual(list(audio), [0.0, 0.25, -0.25])
