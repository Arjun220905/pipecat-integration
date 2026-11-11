# MeetStream × Pipecat

Run a Pipecat voice agent in Google Meet, Zoom, or Microsoft Teams. MeetStream
joins the meeting and streams audio to the bridge. The default pipeline uses
Deepgram for speech recognition and synthesis, Gemini for replies, and Silero
for voice activity detection.

For installation and your first meeting, start with the [Quickstart](Quickstart.md).

## Setup

Install Python 3.11–3.13, Docker with Compose, and ngrok for automatic public
tunneling. On Windows, use Docker Desktop with the Hyper-V backend and Linux
containers. Clone the repository and open its directory.

```sh
git clone https://github.com/meetstream-ai/pipecat-integration.git
cd pipecat-integration
```

| Action | Mac/Linux | Windows PowerShell or Command Prompt |
| --- | --- | --- |
| Start or create initial settings | `./start` | `.\start.cmd` |
| Invite a bot | `./start 'MEETING_URL'` | `.\start.cmd "MEETING_URL"` |
| Check status | `./status` | `.\status.cmd` |
| Remove bots and stop | `./stop` | `.\stop.cmd` |

On first launch the launcher creates `.env`. Set `MEETSTREAM_API_KEY`,
`DEEPGRAM_API_KEY`, and `GOOGLE_API_KEY`. For ngrok, set `NGROK_AUTHTOKEN` and
leave `PUBLIC_URL` blank initially. For your own proxy, set `PUBLIC_URL` to its
HTTPS URL and leave the ngrok token blank. Forward WebSocket upgrades on
`/bridge` and `/bridge/audio` and omit query strings from access logs.
Keep the generated `BRIDGE_TOKEN` and `DEPLOYMENT_ID` private.

Use `--port 8080` on the start command to change the loopback port. Mac/Linux
developers can use `./start --native` to run outside Docker. Stop the deployment
before switching modes or applying configuration changes.

## Audio transport

```text
Meeting audio → MeetStream → /bridge/audio → Pipecat STT → LLM → TTS
Meeting playback ← MeetStream ← /bridge ← Pipecat output
```

MeetStream opens two authenticated WebSockets. Each announces its bot ID in a
`ready` message. The bridge pairs them within 30 seconds using the credential
for the registered bot. Incoming PCM16 is resampled to 16 kHz for the pipeline.
Outgoing audio is paced as 48 kHz mono PCM16. A socket disconnect or a stalled
audio consumer ends the session and schedules bot removal.

Pipecat interruptions clear queued speech and send `interrupt` to MeetStream.
The bridge sends `resume_audio` before the next reply. Google Meet ignores resume.
Zoom waits for a matching successful acknowledgement before sending new audio.
Teams resumes playback after the command. Mixed meeting audio and capture buffers
can affect overlap handling and interruption latency.

To customize the voice pipeline, edit `agent.create_agent()` or pass an
`agent_factory` to `server.create_app()`. Include `transport.drain()` after
STT/VAD to release raw audio credits. Preserve TTS context IDs so interrupted
audio can be discarded.

## Operations

Compose runs one bridge and one cleanup worker with a shared SQLite volume.
The default limit is four sessions, up to one hour each, with a five-minute
join timeout. Each start command with a meeting URL creates another bot.
A fresh bot starts a fresh conversation.

- Use `/health` for process liveness and `/ready` for readiness. Provider
  credentials and quota can still affect a call.
- `POST /meetings` and `GET /metrics` require `Authorization: Bearer BRIDGE_TOKEN`.
  The creation API accepts a UUID-hex `Idempotency-Key`. Use this token only
  for trusted operators. Meeting sockets use separate expiring credentials.
- Find the Compose project with `docker compose ls`. Inspect services using
  `docker compose -p PROJECT logs --tail 100 bridge reaper`. Launcher and tunnel
  logs live in `.run/`.
- If stop reports unconfirmed cleanup, keep Docker running and retry after
  connectivity returns. Preserve the volume and `DEPLOYMENT_ID` until removal
  is confirmed. For continuous operation, schedule the image's `sweep` command
  off-host with only `MEETSTREAM_API_KEY` and `DEPLOYMENT_ID`.

Keep `.env`, `.run/`, and the SQLite volume private. The bridge requests stored
audio and video recording off by default. Zoom can require capture permission
and show a recording indicator. The voice providers process meeting audio.

[MeetStream audio protocol](https://docs.meetstream.ai/guides/websockets/real-time-audio-streaming) ·
[Meeting controls](https://docs.meetstream.ai/guides/websockets/meeting-control-patterns) ·
[Pipecat](https://github.com/pipecat-ai/pipecat) · [MIT license](LICENSE)
