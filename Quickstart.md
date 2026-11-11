# Bring a voice assistant into your meeting

The assistant joins your meeting, listens when you speak, and replies out loud.
You can use it in Google Meet, Zoom, or Microsoft Teams.

## Get set up

Open the [project on GitHub](https://github.com/meetstream-ai/pipecat-integration),
choose **Code → Download ZIP**, and extract the folder. Open Terminal on Mac or
Linux, or PowerShell on Windows. Type `cd` followed by a space and the path to
the extracted folder. Put quotes around the path if it contains spaces.

Install [Python 3.11–3.13](https://www.python.org/downloads/),
[Docker with Compose](https://docs.docker.com/get-started/get-docker/), and the
[ngrok command-line tool](https://ngrok.com/download). Start Docker before you
continue. On Windows, use Docker Desktop with
**Hyper-V** and **Linux containers**. This setup requires an Intel or AMD PC
running supported 64-bit Windows Pro, Enterprise, or Education. [Docker's Windows setup guide](https://docs.docker.com/desktop/setup/install/windows-install/)

Run the command for your computer once to create the settings file.

| Mac or Linux | Windows |
| --- | --- |
| `./start` | `.\start.cmd` |

Open `.env` in a text editor and fill in these settings. On Mac, `open -t .env`
opens the file. On Windows, use `notepad .env`. An API key or token is a secret
code you get from the service's account dashboard.

| Setting | What it connects |
| --- | --- |
| `MEETSTREAM_API_KEY` | The assistant to your meeting |
| `DEEPGRAM_API_KEY` | Speech recognition and spoken replies |
| `GOOGLE_API_KEY` | Gemini for generating answers |
| `NGROK_AUTHTOKEN` | The meeting service to your computer |

Save the file as `.env` and keep it private. Leave the other settings as they
are unless your teammate gave you a specific configuration. These services can
charge for usage.

## Invite the assistant

Copy the invitation link for your meeting. Replace the example link below with
yours, keeping the quotes.

Run this on Mac or Linux.

```sh
./start 'https://meet.google.com/your-meeting-code'
```

Run this on Windows.

```powershell
.\start.cmd "https://meet.google.com/your-meeting-code"
```

The first launch may take several minutes. For Zoom, use the full participant
invitation link including its passcode. Admit **MeetStream Pipecat** if it waits
in the lobby. Zoom may ask the host to allow recording permission for live audio
and show a recording indicator. Let participants know the assistant processes
meeting speech.

Speak first. Try asking “What is the capital of France?” Keep the computer awake
and connected while the assistant is in the meeting.

## Check or stop the assistant

| Action | Mac or Linux | Windows |
| --- | --- | --- |
| Check whether the project is running | `./status` | `.\status.cmd` |
| Remove your meeting bots and shut down | `./stop` | `.\stop.cmd` |

Run the stop command when finished and wait for shutdown confirmation. If it
reports incomplete cleanup, keep Docker running and retry after restoring your
connection. If you change a setting, stop the project before starting again.

If the assistant is silent, check that it was admitted, your microphone works,
and your service accounts have usage credit. For configuration and troubleshooting
details, see the [README](README.md).
