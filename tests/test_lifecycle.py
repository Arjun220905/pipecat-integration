import argparse
import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from meetstream_pipecat.bots import create_reserved, reconcile_once, stop_bots, sweep_remote
from meetstream_pipecat.store import Store
from scripts import manage


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = Store(self.directory.name)
        self.sid, self.token = self.store.reserve("https://meet.google.com/test", "tenant-a")
        self.store.bind(self.sid, "test-bot")

    def tearDown(self):
        self.directory.cleanup()

    def client(self, handler):
        return httpx.Client(base_url="https://api.example/", transport=httpx.MockTransport(handler))

    def test_isolation_expiry_and_revocation(self):
        sid2, token2 = self.store.reserve("https://meet.google.com/other", "tenant-b")
        self.store.bind(sid2, "other-bot")
        self.assertIsNone(self.store.authorize(self.sid, token2))
        self.assertIsNone(self.store.authorize(sid2, self.token))
        self.assertEqual(self.store.authorize(self.sid, self.token)["bot_id"], "test-bot")
        self.store.stopping(self.sid, "operator_stop")
        self.assertIsNone(self.store.authorize(self.sid, self.token))
        with self.store.connection() as db:
            db.execute("UPDATE meetings SET expires=? WHERE id=?", (time.time() - 1, sid2))
        self.assertIsNone(self.store.authorize(sid2, token2))
        self.assertEqual(self.store.path.stat().st_mode & 0o777, 0o600)

    def test_remote_stop_requires_confirmation(self):
        methods = []

        def api(request):
            methods.append(request.method)
            return (
                httpx.Response(202, json={"status": "stop_requested"})
                if request.method == "POST"
                else httpx.Response(200, json={"status": "stopped"})
            )

        with (
            patch("meetstream_pipecat.bots.api_client", return_value=self.client(api)),
            patch("meetstream_pipecat.bots.time.sleep"),
        ):
            self.assertEqual(stop_bots(store=self.store), [])
        self.assertEqual(methods, ["POST", "GET"])
        self.assertEqual(self.store.active(), [])
        self.assertIsNone(self.store.authorize(self.sid, self.token))

    def test_naturally_ended_meeting_is_confirmed_without_stop_request(self):
        with patch(
            "meetstream_pipecat.bots.api_client",
            return_value=self.client(
                lambda _: httpx.Response(
                    200, json={"status": "not_requested", "bot_status": "Done"}
                )
            ),
        ):
            self.assertEqual(stop_bots(store=self.store, timeout=0), [])
        self.assertEqual(self.store.active(), [])
        self.assertIsNone(self.store.authorize(self.sid, self.token))

    def test_failed_or_unconfirmed_stop_is_retained(self):
        for response in (
            httpx.Response(503),
            httpx.Response(200, json={"status": "not_requested", "bot_status": "InMeeting"}),
            httpx.Response(202, json={"status": "stop_requested"}),
        ):
            with patch(
                "meetstream_pipecat.bots.api_client",
                return_value=self.client(lambda _, r=response: r),
            ):
                self.assertEqual(stop_bots(store=self.store, timeout=0), ["test-bot"])
            self.assertEqual(self.store.get(self.sid)["state"], "stopping")

    def test_unknown_bot_cannot_be_stopped_by_this_registry(self):
        with self.assertRaisesRegex(ValueError, "not registered"):
            stop_bots("other-deployment-bot", store=self.store)

    def test_worker_crash_reconciliation(self):
        self.store.claim(self.sid, "dead-worker")
        with self.store.connection() as db:
            db.execute("UPDATE meetings SET heartbeat=? WHERE id=?", (time.time() - 60, self.sid))
        requests = []

        def api(request):
            requests.append(request.url.path)
            return httpx.Response(200, json={"status": "stopped"})

        with patch("meetstream_pipecat.bots.api_client", return_value=self.client(api)):
            self.assertEqual(reconcile_once(self.store), 0)
        self.assertEqual(requests, ["/test-bot/remove_bot"])
        self.assertEqual(self.store.active(), [])

    def test_restart_revokes_previous_worker_and_deadline_cleanup(self):
        self.store.claim(self.sid, "old-worker")
        self.store.recover("new-worker")
        self.assertIsNone(self.store.authorize(self.sid, self.token))
        self.assertEqual(self.store.get(self.sid)["reason"], "worker_restarted")
        sid, _ = self.store.reserve("https://meet.google.com/deadline")
        self.store.bind(sid, "expired-bot")
        with self.store.connection() as db:
            db.execute("UPDATE meetings SET expires=? WHERE id=?", (time.time() - 1, sid))
        with patch(
            "meetstream_pipecat.bots.api_client",
            return_value=self.client(lambda _: httpx.Response(200, json={"status": "stopped"})),
        ):
            self.assertEqual(reconcile_once(self.store), 0)
        self.assertEqual(self.store.active(), [])

    def test_ambiguous_creation_reuses_saved_idempotency_key(self):
        sid, _ = self.store.reserve("https://meet.google.com/ambiguous")
        self.store.save_payload(sid, {"meeting_link": "https://meet.google.com/ambiguous"})
        keys = []

        def api(request):
            keys.append(request.headers["Idempotency-Key"])
            if len(keys) == 1:
                raise httpx.ReadTimeout("response lost after remote creation", request=request)
            return httpx.Response(507, json={"bot_id": "created-once"})

        with self.client(api) as client:
            with self.assertRaises(httpx.ReadTimeout):
                create_reserved(self.store, sid, client)
            self.assertEqual(create_reserved(self.store, sid, client), "created-once")
        self.assertEqual(keys, [sid, sid])
        self.assertEqual(self.store.get(sid)["bot_id"], "created-once")

    def test_interrupted_creation_is_recovered_and_removed(self):
        self.store.finished(self.sid)
        sid, _ = self.store.reserve("https://meet.google.com/ambiguous")
        self.store.save_payload(sid, {"meeting_link": "https://meet.google.com/ambiguous"})
        created = self.store.get(sid)["created"]
        paths = []

        def api(request):
            paths.append(request.url.path)
            if request.url.path.endswith("create_bot"):
                return httpx.Response(507, json={"bot_id": "recovered-bot"})
            return httpx.Response(200, json={"status": "stopped"})

        with patch("meetstream_pipecat.bots.api_client", return_value=self.client(api)):
            self.assertEqual(reconcile_once(self.store, now=created + 31), 0)
        self.assertEqual(paths, ["/create_bot", "/recovered-bot/remove_bot"])
        self.assertEqual(self.store.active(), [])

    def test_capacity_is_atomic_and_legacy_credentials_are_retired(self):
        with self.assertRaisesRegex(ValueError, "capacity"):
            self.store.reserve("https://meet.google.com/full", limit=1)
        legacy = Path(self.directory.name) / "bots.json"
        legacy.write_text(
            json.dumps({"legacy-bot": {"meeting_url": "https://meet.google.com/old"}})
        )
        store = Store(self.directory.name)
        row = next(r for r in store.active() if r["bot_id"] == "legacy-bot")
        self.assertEqual(row["state"], "stopping")
        self.assertEqual(row["token_hash"], "")

    def test_stateless_sweep_never_stops_other_deployments_or_unexpired_meetings(self):
        deployment = "a" * 32
        stops = []

        def attrs(dep, expires):
            return {
                "integration": "meetstream-pipecat",
                "deployment_id": dep,
                "expires_at": str(expires),
            }

        def api(request):
            if request.method == "POST":
                stops.append(request.url.path)
                return httpx.Response(200, json={"status": "stopped"})
            if request.url.params.get("status") != "InMeeting":
                return httpx.Response(200, json={"bots": [], "hasNextPage": False})
            return httpx.Response(
                200,
                json={
                    "bots": [
                        {"bot_id": "expired-owned", "custom_attributes": attrs(deployment, 10)},
                        {"bot_id": "unexpired-owned", "custom_attributes": attrs(deployment, 1000)},
                        {"bot_id": "other-deployment", "custom_attributes": attrs("b" * 32, 10)},
                    ],
                    "hasNextPage": False,
                },
            )

        with self.client(api) as client:
            self.assertEqual(sweep_remote(client, deployment, now=100), 0)
        self.assertEqual(stops, ["/expired-owned/remove_bot"])

    def test_remote_cleanup_failure_stays_unready_between_sweeps(self):
        with (
            patch.dict(os.environ, {"DEPLOYMENT_ID": "a" * 32}),
            patch("meetstream_pipecat.bots.api_client"),
            patch("meetstream_pipecat.bots.sweep_remote", return_value=1) as sweep,
        ):
            self.assertEqual(reconcile_once(self.store, now=1000), 1)
            self.assertEqual(reconcile_once(self.store, now=1005), 1)
            self.assertEqual(self.store.meta("cleanup_pending"), 1)
            self.assertEqual(sweep.call_count, 1)
            sweep.return_value = 0
            self.assertEqual(reconcile_once(self.store, now=1061), 0)

    def test_stateless_sweep_detects_nonadvancing_cursor(self):
        def api(request):
            return httpx.Response(200, json={"bots": [], "hasNextPage": True, "nextCursor": "same"})

        with self.client(api) as client, self.assertRaisesRegex(ValueError, "pagination"):
            sweep_remote(client, "a" * 32)


class LauncherTests(unittest.TestCase):
    def test_stale_pid_never_signals_unrelated_process(self):
        result = subprocess.CompletedProcess([], 0, stdout="unrelated-program --port 8000")
        with (
            patch.object(manage.subprocess, "run", return_value=result),
            patch.object(manage.os, "killpg") as kill,
        ):
            manage.terminate({"pid": 123, "marker": "unique-bridge-instance"})
            kill.assert_not_called()

    def test_first_start_creates_private_env(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env.example").write_text(
                "BRIDGE_TOKEN=\nGOOGLE_API_KEY=\nDEEPGRAM_API_KEY=\n"
            )
            with patch.object(manage, "ROOT", root), patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(ValueError, "DEEPGRAM_API_KEY, GOOGLE_API_KEY"):
                    manage.start(argparse.Namespace(meeting_url=None, port=8000, native=True))
            self.assertEqual((root / ".env").stat().st_mode & 0o777, 0o600)

    def test_repeated_start_does_not_spawn_duplicate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env").write_text("")
            with patch.dict(
                os.environ,
                {
                    "DEEPGRAM_API_KEY": "test",
                    "GOOGLE_API_KEY": "test",
                    "MEETSTREAM_API_KEY": "test",
                    "BRIDGE_TOKEN": "test-token-value-long",
                    "DEPLOYMENT_ID": "a" * 32,
                },
            ):
                with (
                    patch.object(manage, "ROOT", root),
                    patch("dotenv.load_dotenv", return_value=False),
                    patch.object(manage, "read_state", return_value={"server": {"pid": 123}}),
                    patch.object(manage, "owned", return_value=True),
                    patch.object(manage, "launch") as launch,
                ):
                    manage.start(argparse.Namespace(meeting_url=None, port=8000, native=True))
                    launch.assert_not_called()
