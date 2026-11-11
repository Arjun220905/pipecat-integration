"""Durable per-meeting credentials and leases, using Python's built-in SQLite."""

import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


def state_dir() -> Path:
    path = Path(os.getenv("MEETSTREAM_STATE_DIR", ".run")).resolve()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


class Store:
    def __init__(self, directory=None):
        self.directory = Path(directory) if directory else state_dir()
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = self.directory / "meetings.sqlite3"
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(descriptor)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS meetings (
                    id TEXT PRIMARY KEY, token_hash TEXT NOT NULL,
                    bot_id TEXT UNIQUE, tenant TEXT NOT NULL, meeting_url TEXT NOT NULL,
                    state TEXT NOT NULL, payload TEXT, created REAL NOT NULL,
                    updated REAL NOT NULL, expires REAL NOT NULL,
                    owner TEXT, heartbeat REAL, reason TEXT
                );
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS meetings_state ON meetings(state);
            """)
        self._migrate()

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA secure_delete=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    def _migrate(self):
        with (self.directory / "migration.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self._migrate_locked()

    def _migrate_locked(self):
        legacy = self.directory / "bots.json"
        if not legacy.exists():
            return
        with self.connection() as db:
            for bot_id, record in json.loads(legacy.read_text()).items():
                now = time.time()
                db.execute(
                    "INSERT OR IGNORE INTO meetings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        uuid4().hex,
                        "",
                        bot_id,
                        "owner",
                        record.get("meeting_url", ""),
                        "stopping",
                        None,
                        now,
                        now,
                        now,
                        None,
                        None,
                        "legacy_credentials_revoked",
                    ),
                )
        legacy.rename(legacy.with_suffix(".migrated"))

    def reserve(self, meeting_url, tenant="owner", *, max_seconds=3600, limit=8, session_id=None):
        if (
            not 60 <= max_seconds <= 18_000
            or not isinstance(tenant, str)
            or not 1 <= len(tenant) <= 100
        ):
            raise ValueError("Invalid meeting lifetime or tenant")
        session_id, token = session_id or uuid4().hex, secrets.token_urlsafe(32)
        if not re.fullmatch(r"[0-9a-f]{32}", session_id):
            raise ValueError("Invalid creation request ID")
        now = time.time()
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            count = db.execute("SELECT count(*) FROM meetings WHERE state != 'stopped'").fetchone()[
                0
            ]
            if count >= limit:
                raise ValueError("Meeting capacity reached; stop an existing meeting first")
            db.execute(
                "INSERT INTO meetings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    session_id,
                    hashlib.sha256(token.encode()).hexdigest(),
                    None,
                    tenant,
                    meeting_url,
                    "creating",
                    None,
                    now,
                    now,
                    now + max_seconds,
                    None,
                    None,
                    None,
                ),
            )
        return session_id, token

    def get(self, session_id):
        with self.connection() as db:
            row = db.execute("SELECT * FROM meetings WHERE id=?", (session_id,)).fetchone()
            return dict(row) if row else None

    def active(self):
        with self.connection() as db:
            return [dict(r) for r in db.execute("SELECT * FROM meetings WHERE state != 'stopped'")]

    def authorize(self, session_id, token):
        if len(session_id) != 32 or len(token) > 128:
            return None
        row = self.get(session_id)
        digest = hashlib.sha256(token.encode()).hexdigest()
        expected = row["token_hash"] if row else "0" * 64
        valid = hmac.compare_digest(digest, expected)
        if (
            not valid
            or not row
            or row["state"] not in {"creating", "active"}
            or row["expires"] <= time.time()
        ):
            return None
        return row

    def save_payload(self, session_id, payload):
        with self.connection() as db:
            db.execute(
                "UPDATE meetings SET payload=?, updated=? WHERE id=? AND state='creating'",
                (json.dumps(payload), time.time(), session_id),
            )

    def bind(self, session_id, bot_id):
        if not isinstance(bot_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", bot_id):
            raise ValueError("Invalid bot_id from MeetStream")
        with self.connection() as db:
            result = db.execute(
                "UPDATE meetings SET bot_id=?, state=CASE WHEN state='creating' THEN 'active' ELSE state END, updated=? WHERE id=? AND (bot_id IS NULL OR bot_id=?)",
                (bot_id, time.time(), session_id, bot_id),
            )
            if result.rowcount != 1:
                raise ValueError("Bot binding conflicts with saved creation")

    def claim(self, session_id, owner):
        with self.connection() as db:
            result = db.execute(
                "UPDATE meetings SET owner=?, heartbeat=?, updated=? WHERE id=? AND state='active' AND (owner IS NULL OR owner=?) AND expires>?",
                (owner, time.time(), time.time(), session_id, owner, time.time()),
            )
            return result.rowcount == 1

    def heartbeat(self, owner):
        with self.connection() as db:
            db.execute(
                "UPDATE meetings SET heartbeat=? WHERE owner=? AND state='active'",
                (time.time(), owner),
            )

    def recover(self, owner):
        with self.connection() as db:
            db.execute(
                "UPDATE meetings SET state='stopping', reason='worker_restarted', updated=? WHERE owner IS NOT NULL AND owner!=? AND state='active'",
                (time.time(), owner),
            )

    def stopping(self, session_id, reason):
        with self.connection() as db:
            db.execute(
                "UPDATE meetings SET state='stopping', reason=?, updated=? WHERE id=? AND state!='stopped'",
                (reason, time.time(), session_id),
            )

    def finished(self, session_id):
        with self.connection() as db:
            db.execute(
                "UPDATE meetings SET state='stopped', token_hash='', payload=NULL, meeting_url='', owner=NULL, heartbeat=NULL, updated=? WHERE id=?",
                (time.time(), session_id),
            )

    def set_meta(self, key, value):
        with self.connection() as db:
            db.execute(
                "INSERT INTO metadata VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def meta(self, key):
        with self.connection() as db:
            row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            return row[0] if row else 0

    def increment(self, key):
        with self.connection() as db:
            db.execute(
                "INSERT INTO metadata VALUES (?,1) ON CONFLICT(key) DO UPDATE SET value=value+1",
                (key,),
            )

    def purge(self):
        with self.connection() as db:
            db.execute(
                "DELETE FROM meetings WHERE state='stopped' AND updated<?", (time.time() - 86400,)
            )
