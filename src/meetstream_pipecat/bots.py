"""Idempotent creation and cleanup, including ambiguous network failures."""

import json
import logging
import os
import re
import time
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx

from .store import Store

logger = logging.getLogger(__name__)
TERMINAL = {"stopped", "force_stopped", "cancelled"}
ENDED_BOT_STATES = {
    "stopped",
    "kicked",
    "notallowed",
    "denied",
    "failed",
    "error",
    "cancelled",
    "mediaprocessing",
    "done",
    "mediaexpired",
}


def stop_confirmed(result):
    return (
        str(result.get("status", "")).casefold() in TERMINAL
        or str(result.get("bot_status", "")).casefold() in ENDED_BOT_STATES
    )


def api_client():
    key = os.environ.get("MEETSTREAM_API_KEY")
    if not key:
        raise ValueError("Set MEETSTREAM_API_KEY for meeting creation and cleanup")
    return httpx.Client(
        base_url="https://api.meetstream.ai/api/v1/bots/",
        headers={"Authorization": f"Token {key}"},
        timeout=5,
    )


def create_reserved(store, session_id, client):
    row = store.get(session_id)
    if row["bot_id"]:
        return row["bot_id"]
    response = client.post(
        "create_bot", json=json.loads(row["payload"]), headers={"Idempotency-Key": session_id}
    )
    if response.status_code not in (200, 201, 507):
        raise ValueError(f"MeetStream create_bot returned HTTP {response.status_code}")
    bot_id = response.json().get("bot_id")
    store.bind(session_id, bot_id)
    return bot_id


def remove_one(store, row, client, *, timeout=0):
    """Never erase a remote bot until the API confirms it has stopped."""
    store.stopping(row["id"], row.get("reason") or "stop_requested")
    if not row["bot_id"]:
        if not row["payload"]:
            store.finished(row["id"])
            return True
        bot_id = create_reserved(store, row["id"], client)
    else:
        bot_id = row["bot_id"]
    response = client.post(f"{bot_id}/remove_bot")
    if response.status_code not in (200, 202):
        return False
    result = response.json()
    deadline = time.monotonic() + timeout
    while not stop_confirmed(result):
        if result.get("status") == "failed" or time.monotonic() >= deadline:
            return False
        time.sleep(0.5)
        response = client.get(f"{bot_id}/stop_status")
        response.raise_for_status()
        result = response.json()
    store.finished(row["id"])
    return True


def stop_bots(bot_id=None, *, timeout=20, store=None):
    store = store or Store()
    rows = [r for r in store.active() if bot_id is None or r["bot_id"] == bot_id]
    if bot_id and not rows:
        raise ValueError("Bot is not registered in this deployment")
    pending = []
    deployment = os.getenv("DEPLOYMENT_ID")
    if not rows and not deployment:
        return pending
    with api_client() as client:
        for row in rows:
            try:
                if remove_one(store, row, client, timeout=timeout):
                    print(f"Stopped meeting {row['bot_id'] or row['id']}")
                else:
                    pending.append(row["bot_id"] or row["id"])
            except (httpx.HTTPError, ValueError):
                pending.append(row["bot_id"] or row["id"])
        if deployment and bot_id is None:
            try:
                if sweep_remote(client, deployment, force=True):
                    pending.append("remote deployment meetings")
            except (httpx.HTTPError, ValueError):
                pending.append("remote deployment scan")
    return pending


def reconcile_once(store=None, *, now=None):
    store = store or Store()
    now = time.time() if now is None else now
    store.set_meta("reaper_heartbeat", now)
    failures = 0
    rows = store.active()
    if rows:
        with api_client() as client:
            for row in rows:
                reason = None
                if row["state"] == "stopping":
                    reason = row["reason"] or "stop_requested"
                elif row["expires"] <= now:
                    reason = "meeting_deadline"
                elif row["owner"] and (row["heartbeat"] or 0) < now - 30:
                    reason = "worker_heartbeat_lost"
                elif row["state"] == "creating" and row["created"] < now - 30:
                    reason = "creation_interrupted"
                elif not row["owner"] and row["created"] < now - int(
                    os.getenv("JOIN_TIMEOUT_SECONDS", "300")
                ):
                    reason = "join_timeout"
                if not reason:
                    continue
                store.stopping(row["id"], reason)
                try:
                    if not remove_one(store, store.get(row["id"]), client):
                        failures += 1
                except (httpx.HTTPError, ValueError):
                    failures += 1
    deployment = os.getenv("DEPLOYMENT_ID")
    remote_failures = store.meta("remote_cleanup_pending") if deployment else 0
    if deployment and now - store.meta("last_remote_sweep") >= 60:
        try:
            with api_client() as client:
                remote_failures = sweep_remote(client, deployment, now=now)
            store.set_meta("last_remote_sweep", now)
        except (httpx.HTTPError, ValueError):
            remote_failures = 1
        store.set_meta("remote_cleanup_pending", remote_failures)
    failures += remote_failures
    store.set_meta("cleanup_pending", failures)
    if failures:
        store.increment("cleanup_failures_total")
        logger.error("Meeting cleanup pending for %d operation(s)", failures)
    store.purge()
    store.set_meta("reaper_heartbeat", time.time())
    return failures


def bot_payload(meeting_url, *, session_id, session_token, expires_at=None, bot_name=None):
    public = urlsplit(os.environ["PUBLIC_URL"])
    meeting = urlsplit(meeting_url)
    bot_name = bot_name or os.getenv("BOT_NAME", "MeetStream Pipecat")
    if (
        not isinstance(bot_name, str)
        or not bot_name.strip()
        or not bot_name.isprintable()
        or len(bot_name) > 80
    ):
        raise ValueError("Bot name must contain 1–80 printable characters")
    if meeting.scheme != "https" or not meeting.hostname or meeting.username:
        raise ValueError("Meeting URL must be HTTPS without embedded credentials")
    if (
        public.scheme != "https"
        or not public.hostname
        or public.username
        or public.query
        or public.fragment
    ):
        raise ValueError("PUBLIC_URL must be HTTPS without credentials, query, or fragment")
    base = urlunsplit(("wss", public.netloc, public.path.rstrip("/"), "", ""))
    query = urlencode({"session": session_id, "token": session_token})
    deployment = os.environ["DEPLOYMENT_ID"]
    if not re.fullmatch(r"[0-9a-f]{32}", deployment):
        raise ValueError("DEPLOYMENT_ID must be a persistent UUID hex string")
    return {
        "meeting_link": meeting_url,
        "bot_name": bot_name,
        "audio_required": os.getenv("RECORD_AUDIO", "false").lower() == "true",
        "video_required": False,
        "custom_attributes": {
            "integration": "meetstream-pipecat",
            "deployment_id": deployment,
            "session_id": session_id,
            "expires_at": str(
                int(expires_at or time.time() + int(os.getenv("MAX_MEETING_SECONDS", "3600")))
            ),
        },
        "automatic_leave": {
            "waiting_room_timeout": 300,
            "no_one_joined_timeout": 300,
            "everyone_left_timeout": 60,
            "voice_inactivity_timeout": 300,
        },
        "live_audio_required": {"websocket_url": f"{base}/bridge/audio?{query}"},
        "socket_connection_url": {"websocket_url": f"{base}/bridge?{query}"},
    }


def sweep_remote(client, deployment, *, now=None, force=False):
    """Stateless expiry cleanup: can run off host without the SQLite volume."""
    if not re.fullmatch(r"[0-9a-f]{32}", deployment):
        raise ValueError("Invalid deployment identity")
    now = time.time() if now is None else now
    pending = 0
    for status in (
        "Joining",
        "InWaitingRoom",
        "InMeeting",
        "RecordingPermissionAllowed",
        "Leaving",
        "Scheduled",
    ):
        params = {
            "custom_attr[integration]": "meetstream-pipecat",
            "custom_attr[deployment_id]": deployment,
            "status": status,
        }
        cursors = set()
        for _ in range(20):
            response = client.get("https://api.meetstream.ai/api/v1/bots", params=params)
            response.raise_for_status()
            data = response.json()
            for item in data.get("bots", []):
                bot_id = item.get("bot_id") or item.get("BotID")
                if not isinstance(bot_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", bot_id):
                    raise ValueError("Invalid bot ID in filtered remote scan")
                attrs = item.get("custom_attributes")
                if not isinstance(attrs, dict):
                    detail = client.get(bot_id)
                    detail.raise_for_status()
                    detail = detail.json().get("bot_details", {})
                    attrs = detail.get("custom_attributes") or (
                        detail.get("RequestPayload") or {}
                    ).get("custom_attributes")
                if (
                    not isinstance(attrs, dict)
                    or attrs.get("integration") != "meetstream-pipecat"
                    or attrs.get("deployment_id") != deployment
                ):
                    continue
                try:
                    expires = int(attrs["expires_at"])
                except (KeyError, ValueError, TypeError) as exc:
                    raise ValueError(
                        "Registered remote meeting has invalid expiry metadata"
                    ) from exc
                if expires > now and not force:
                    continue
                result = client.post(f"{bot_id}/remove_bot")
                if result.status_code not in (200, 202) or not stop_confirmed(result.json()):
                    pending += 1
            if not data.get("hasNextPage"):
                break
            cursor = data.get("nextCursor")
            if not cursor or cursor in cursors:
                raise ValueError("Remote meeting pagination did not advance")
            cursors.add(cursor)
            params["cursor"] = cursor
        else:
            raise ValueError("Remote meeting scan exceeded its page bound")
    return pending
