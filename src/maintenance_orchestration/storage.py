"""检修资源替代编排的 SQLite 模式和事务辅助。"""

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
    role TEXT NOT NULL CHECK(role IN ('planner','dispatcher','risk','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_batches (
    resource_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK(kind IN ('vessel','lifting_window','spare_kit','crew')),
    batch_id TEXT NOT NULL,
    capability_mw TEXT,
    sea_state_max_m TEXT,
    available_from TEXT,
    available_to TEXT,
    certifications_json TEXT NOT NULL DEFAULT '[]',
    compat_models_json TEXT NOT NULL DEFAULT '[]',
    capacity INTEGER NOT NULL DEFAULT 1,
    note TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL DEFAULT 1,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by TEXT NOT NULL REFERENCES mo_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_resources_kind_batch
ON resource_batches(kind, batch_id);

CREATE TABLE IF NOT EXISTS maintenance_operations (
    operation_id TEXT PRIMARY KEY,
    turbine_model TEXT NOT NULL,
    sea_window_starts_at TEXT NOT NULL,
    sea_window_ends_at TEXT NOT NULL,
    depends_on_json TEXT NOT NULL DEFAULT '[]',
    state TEXT NOT NULL DEFAULT 'registered'
        CHECK(state IN ('registered','started','completed','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    started_at TEXT,
    created_by TEXT NOT NULL REFERENCES mo_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_requirements (
    requirement_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL REFERENCES maintenance_operations(operation_id),
    kind TEXT NOT NULL CHECK(kind IN ('vessel','lifting_window','spare_kit','crew')),
    minimum_certification TEXT,
    minimum_capability_mw TEXT,
    required_model TEXT,
    sea_state_limit_m TEXT,
    preference_order_json TEXT NOT NULL,
    essential INTEGER NOT NULL DEFAULT 1 CHECK(essential IN (0,1)),
    ordinal INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_requirements_operation
ON operation_requirements(operation_id, ordinal);

CREATE TABLE IF NOT EXISTS maintenance_plans (
    plan_id TEXT PRIMARY KEY,
    operation_ids_json TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'frozen' CHECK(state IN ('frozen','retired')),
    frozen_version INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES mo_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plan_candidate_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL REFERENCES maintenance_plans(plan_id),
    input_sha256 TEXT NOT NULL,
    resource_versions_json TEXT NOT NULL,
    feasible INTEGER NOT NULL CHECK(feasible IN (0,1)),
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES mo_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(plan_id, input_sha256)
);

CREATE TABLE IF NOT EXISTS maintenance_orders (
    order_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES maintenance_plans(plan_id),
    candidate_run_id INTEGER NOT NULL REFERENCES plan_candidate_runs(run_id),
    candidate_ordinal INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'confirmed' CHECK(state IN ('confirmed','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    confirmed_by TEXT NOT NULL REFERENCES mo_users(user_id),
    confirmed_at TEXT NOT NULL,
    cancelled_by TEXT REFERENCES mo_users(user_id),
    cancelled_at TEXT,
    cancel_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_orders_plan
ON maintenance_orders(plan_id, state);

CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL REFERENCES maintenance_orders(order_id),
    operation_id TEXT NOT NULL REFERENCES maintenance_operations(operation_id),
    requirement_id TEXT NOT NULL,
    resource_id TEXT NOT NULL REFERENCES resource_batches(resource_id),
    batch_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    essential INTEGER NOT NULL CHECK(essential IN (0,1)),
    quantity INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','consumed','released')),
    ordinal INTEGER NOT NULL,
    resource_revision INTEGER NOT NULL,
    consumed_at TEXT,
    released_at TEXT,
    release_reason TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    UNIQUE(order_id, requirement_id)
);

CREATE INDEX IF NOT EXISTS idx_reservations_resource
ON resource_reservations(resource_id, state);

CREATE INDEX IF NOT EXISTS idx_reservations_operation
ON resource_reservations(operation_id, state);

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
