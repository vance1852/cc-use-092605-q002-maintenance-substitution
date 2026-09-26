"""检修资源编排服务的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS mo_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('planner','coordinator','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_batches (
    batch_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK(kind IN ('mother-vessel','lifting-window','spare-kit','crew')),
    name TEXT NOT NULL,
    qualification TEXT,
    compatible_parts_json TEXT,
    max_wave_m TEXT,
    max_wind_ms TEXT,
    window_start TEXT,
    window_end TEXT,
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    available INTEGER NOT NULL CHECK(available >= 0),
    revision INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','suspended','retired')),
    created_by TEXT NOT NULL REFERENCES mo_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS maintenance_jobs (
    job_id TEXT PRIMARY KEY,
    turbine_id TEXT NOT NULL,
    title TEXT NOT NULL,
    depends_on_json TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    max_wave_m TEXT NOT NULL,
    max_wind_ms TEXT NOT NULL,
    min_qualification TEXT NOT NULL,
    required_parts_json TEXT NOT NULL,
    degradation_json TEXT NOT NULL,
    resource_needs_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'planned'
        CHECK(state IN ('planned','confirmed','started','completed','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES mo_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_dependencies (
    job_id TEXT NOT NULL REFERENCES maintenance_jobs(job_id),
    depends_on_job_id TEXT NOT NULL REFERENCES maintenance_jobs(job_id),
    PRIMARY KEY (job_id, depends_on_job_id)
);

CREATE TABLE IF NOT EXISTS orchestration_requests (
    request_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    job_ids_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    frozen_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'generated' CHECK(state IN ('generated','confirmed','cancelled')),
    confirmed_combination_id INTEGER,
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES mo_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestration_combinations (
    combination_id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL REFERENCES orchestration_requests(request_id),
    rank INTEGER NOT NULL,
    score INTEGER NOT NULL,
    assignments_json TEXT NOT NULL,
    trade_offs_json TEXT NOT NULL,
    unmet_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_combinations_request
ON orchestration_combinations(request_id, rank);

CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL REFERENCES orchestration_requests(request_id),
    combination_id INTEGER NOT NULL REFERENCES orchestration_combinations(combination_id),
    job_id TEXT NOT NULL REFERENCES maintenance_jobs(job_id),
    kind TEXT NOT NULL,
    batch_id TEXT NOT NULL REFERENCES resource_batches(batch_id),
    batch_revision INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','consumed','released')),
    created_at TEXT NOT NULL,
    consumed_at TEXT,
    released_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_reservations_job
ON resource_reservations(job_id, state);

CREATE INDEX IF NOT EXISTS idx_reservations_batch
ON resource_reservations(batch_id, state);

CREATE INDEX IF NOT EXISTS idx_reservations_request
ON resource_reservations(request_id, state);

CREATE TABLE IF NOT EXISTS mo_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS mo_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mo_audit_entity
ON mo_audit_events(entity_type, entity_id, event_id);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)
