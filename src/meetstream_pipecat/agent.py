"""Reference STT → LLM → TTS agent; replace this pipeline with your own."""

import os

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.deepgram.tts import DeepgramTTSService
from pipecat.services.google.llm import GoogleLLMService
from pipecat.turns.user_stop.speech_timeout_user_turn_stop_strategy import (
    SpeechTimeoutUserTurnStopStrategy,
)
from pipecat.turns.user_turn_strategies import UserTurnStrategies

from .flow import ContextWindow
from .transport import MeetStreamTransport


def create_agent(transport: MeetStreamTransport) -> PipelineWorker:
    context = LLMContext()
    aggregators = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=SileroVADAnalyzer(params=VADParams(start_secs=0.2, stop_secs=0.2)),
            user_turn_strategies=UserTurnStrategies(
                stop=[SpeechTimeoutUserTurnStopStrategy(user_speech_timeout=0.6)]
            ),
        ),
    )
    stt = DeepgramSTTService(api_key=os.environ["DEEPGRAM_API_KEY"], sample_rate=16000)
    llm = GoogleLLMService(
        api_key=os.environ["GOOGLE_API_KEY"],
        retry_on_timeout=True,
        retry_timeout_secs=3,
        http_options={"retry_options": {"attempts": 2, "initial_delay": 0.25, "max_delay": 1}},
        settings=GoogleLLMService.Settings(
            model=os.getenv("LLM_MODEL", "gemini-3.5-flash-lite"),
            max_tokens=192,
            system_instruction=(
                "You are a helpful voice assistant participating in a live meeting. "
                "Answer in one or two concise, natural sentences. Do not use markdown. "
                "Listen first and respond when a participant speaks to you. "
                "If interrupted, listen and answer the new question."
            ),
        ),
    )
    tts = DeepgramTTSService(
        api_key=os.environ["DEEPGRAM_API_KEY"],
        sample_rate=48000,
        settings=DeepgramTTSService.Settings(voice=os.getenv("TTS_VOICE", "aura-2-helena-en")),
    )
    return PipelineWorker(
        Pipeline(
            [
                transport.input(),
                stt,
                aggregators.user(),
                transport.drain(),
                ContextWindow(),
                llm,
                tts,
                transport.output(),
                aggregators.assistant(),
            ]
        ),
        params=PipelineParams(audio_in_sample_rate=16000, audio_out_sample_rate=48000),
        idle_timeout_secs=300,
    )
