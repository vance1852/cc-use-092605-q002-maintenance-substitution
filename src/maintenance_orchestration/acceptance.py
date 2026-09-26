"""贯通资源批次、作业登记、候选编排、原子确认和预留释放的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import OrchestrationService


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = OrchestrationService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (("plan", "planner"), ("lead", "coordinator"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    window = {"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-10T00:00:00Z"}
    service.register_batch("plan", {"batch_id": "vessel-a", "kind": "mother-vessel", "name": "母船一", "quantity": 2, "max_wave_m": "2.0", "max_wind_ms": "12", **window})
    service.register_batch("plan", {"batch_id": "vessel-b", "kind": "mother-vessel", "name": "母船二", "quantity": 1, "max_wave_m": "1.2", "max_wind_ms": "10", **window})
    service.register_batch("plan", {"batch_id": "crane-a", "kind": "lifting-window", "name": "吊装窗口一", "quantity": 2, "max_wave_m": "2.0", "max_wind_ms": "12", **window})
    service.register_batch("plan", {"batch_id": "kit-a", "kind": "spare-kit", "name": "备件包一", "quantity": 4, "compatible_parts": ["blade-bearing-18mw", "yaw-motor-18mw"]})
    service.register_batch("plan", {"batch_id": "kit-b", "kind": "spare-kit", "name": "备件包二", "quantity": 2, "compatible_parts": ["yaw-motor-18mw", "converter-module-18mw"]})
    service.register_batch("plan", {"batch_id": "crew-a", "kind": "crew", "name": "班组一", "quantity": 1, "qualification": "A"})
    service.register_batch("plan", {"batch_id": "crew-b", "kind": "crew", "name": "班组二", "quantity": 2, "qualification": "B"})
    degradation = [
        {"rank": 1, "qualification": "A"},
        {"rank": 2, "qualification": "B", "note": "需增派安全员"},
    ]
    service.register_job("plan", {
        "job_id": "job-001", "turbine_id": "turbine-18mw-01", "title": "主轴轴承更换",
        "window_start": "2026-10-03T00:00:00Z", "window_end": "2026-10-05T00:00:00Z",
        "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
        "required_parts": ["blade-bearing-18mw"], "degradation_order": degradation,
    })
    service.register_job("plan", {
        "job_id": "job-002", "turbine_id": "turbine-18mw-02", "title": "偏航电机更换",
        "depends_on": ["job-001"],
        "window_start": "2026-10-05T00:00:00Z", "window_end": "2026-10-07T00:00:00Z",
        "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
        "required_parts": ["yaw-motor-18mw"], "degradation_order": degradation,
    })
    payload = {"request_id": "req-001", "job_ids": ["job-001", "job-002"], "idempotency_key": "orch-001", "max_candidates": 3}
    generated = service.generate_orchestration("plan", payload)
    assert service.generate_orchestration("plan", payload) == generated
    preferred = generated["combinations"][0]
    confirmed = service.confirm_orchestration("lead", "req-001", preferred["combination_id"], "confirm-001")
    replayed = service.confirm_orchestration("lead", "req-001", preferred["combination_id"], "confirm-001")
    started = service.start_job("lead", "job-001", 2)
    cancelled = service.cancel_orchestration("lead", "req-001", "吊装船临时故障，集中检修窗口调整")
    completed = service.complete_job("lead", "job-001", 3)
    job_001 = service.job("audit", "job-001")
    job_002 = service.job("audit", "job-002")
    vessel_a = service.batch("audit", "vessel-a")
    crew_b = service.batch("audit", "crew-b")
    result = {
        "status": "ok",
        "request_id": generated["request_id"],
        "candidates": len(generated["combinations"]),
        "preferred_trade_offs": preferred["trade_offs"],
        "confirmed_reservations": len(confirmed["reservations"]),
        "confirm_replayed": replayed == confirmed,
        "started": started,
        "cancelled": cancelled,
        "completed": completed,
        "job_001_state": job_001["state"],
        "job_001_reservation_batches": [
            {"batch_id": item["batch_id"], "batch_revision": item["batch_revision"], "state": item["state"]}
            for item in job_001["reservations"]
        ],
        "job_002_state": job_002["state"],
        "vessel_a_available": vessel_a["available"],
        "crew_b_available": crew_b["available"],
        "audit": service.audit_chain("audit"),
        "workspace": workspace.name,
    }
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行检修资源替代编排离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
