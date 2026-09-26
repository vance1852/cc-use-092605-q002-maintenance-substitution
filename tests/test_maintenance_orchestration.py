from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from maintenance_orchestration.api import JsonApplication
from maintenance_orchestration.clock import FrozenClock, parse_utc
from maintenance_orchestration.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from maintenance_orchestration.models import MaintenanceJob, ResourceBatch
from maintenance_orchestration.planning import (
    BatchSpec,
    JobSpec,
    degradation_rank,
    eligibility_failure,
    generate_candidates,
)
from maintenance_orchestration.service import OrchestrationService


def _job_spec(**overrides) -> JobSpec:
    values = {
        "job_id": "job-1",
        "window_start": parse_utc("2026-10-03T00:00:00Z"),
        "window_end": parse_utc("2026-10-05T00:00:00Z"),
        "max_wave_m": Decimal("1.5"),
        "max_wind_ms": Decimal("10"),
        "min_qualification": "B",
        "required_parts": ("blade-bearing-18mw",),
        "degradation": (),
        "needs": (),
    }
    values.update(overrides)
    if not values["degradation"]:
        values["degradation"] = MaintenanceJob.from_dict({
            "job_id": "job-1", "turbine_id": "t-1", "title": "t",
            "window_start": "2026-10-03T00:00:00Z", "window_end": "2026-10-05T00:00:00Z",
            "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
        }).degradation
    if not values["needs"]:
        values["needs"] = MaintenanceJob.from_dict({
            "job_id": "job-1", "turbine_id": "t-1", "title": "t",
            "window_start": "2026-10-03T00:00:00Z", "window_end": "2026-10-05T00:00:00Z",
            "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
        }).resource_needs
    return JobSpec(**values)


def _batch_spec(batch_id: str, kind: str, **overrides) -> BatchSpec:
    values = {
        "batch_id": batch_id,
        "kind": kind,
        "revision": 1,
        "available": 1,
        "qualification": None,
        "compatible_parts": (),
        "max_wave_m": None,
        "max_wind_ms": None,
        "window_start": None,
        "window_end": None,
    }
    if kind in ("mother-vessel", "lifting-window"):
        values.update({
            "max_wave_m": Decimal("2.0"),
            "max_wind_ms": Decimal("12"),
            "window_start": parse_utc("2026-10-01T00:00:00Z"),
            "window_end": parse_utc("2026-10-10T00:00:00Z"),
        })
    if kind == "crew":
        values["qualification"] = "A"
    if kind == "spare-kit":
        values["compatible_parts"] = ("blade-bearing-18mw",)
    values.update(overrides)
    return BatchSpec(**values)


class ModelTests(unittest.TestCase):
    def test_crew_batch_requires_known_qualification(self) -> None:
        with self.assertRaises(ValidationFailed):
            ResourceBatch.from_dict({"batch_id": "c-1", "kind": "crew", "name": "班组", "quantity": 1})
        with self.assertRaises(ValidationFailed):
            ResourceBatch.from_dict({"batch_id": "c-1", "kind": "crew", "name": "班组", "quantity": 1, "qualification": "D"})
        batch = ResourceBatch.from_dict({"batch_id": "c-1", "kind": "crew", "name": "班组", "quantity": 1, "qualification": "b"})
        self.assertEqual(batch.qualification, "B")

    def test_spare_kit_requires_compatible_parts(self) -> None:
        with self.assertRaises(ValidationFailed):
            ResourceBatch.from_dict({"batch_id": "k-1", "kind": "spare-kit", "name": "备件", "quantity": 1, "compatible_parts": []})

    def test_vessel_requires_window_and_sea_state(self) -> None:
        with self.assertRaises(ValidationFailed):
            ResourceBatch.from_dict({"batch_id": "v-1", "kind": "mother-vessel", "name": "母船", "quantity": 1, "max_wave_m": "2", "max_wind_ms": "10"})
        with self.assertRaises(ValidationFailed):
            ResourceBatch.from_dict({
                "batch_id": "v-1", "kind": "mother-vessel", "name": "母船", "quantity": 1,
                "max_wave_m": "2", "max_wind_ms": "10",
                "window_start": "2026-10-05T00:00:00Z", "window_end": "2026-10-01T00:00:00Z",
            })

    def test_job_degradation_must_be_consecutive_and_above_floor(self) -> None:
        base = {
            "job_id": "j-1", "turbine_id": "t-1", "title": "t",
            "window_start": "2026-10-03T00:00:00Z", "window_end": "2026-10-05T00:00:00Z",
            "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
        }
        with self.assertRaises(ValidationFailed):
            MaintenanceJob.from_dict({**base, "degradation_order": [{"rank": 2, "qualification": "A"}]})
        with self.assertRaises(ValidationFailed):
            MaintenanceJob.from_dict({**base, "degradation_order": [{"rank": 1, "qualification": "C"}]})
        with self.assertRaises(ValidationFailed):
            MaintenanceJob.from_dict({**base, "degradation_order": [
                {"rank": 1, "qualification": "A"}, {"rank": 2, "qualification": "A"},
            ]})
        job = MaintenanceJob.from_dict({**base, "degradation_order": [
            {"rank": 1, "qualification": "A"}, {"rank": 2, "qualification": "B", "note": "需增派安全员"},
        ]})
        self.assertEqual(job.degradation[1].note, "需增派安全员")

    def test_job_needs_must_cover_four_resource_kinds(self) -> None:
        base = {
            "job_id": "j-1", "turbine_id": "t-1", "title": "t",
            "window_start": "2026-10-03T00:00:00Z", "window_end": "2026-10-05T00:00:00Z",
            "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
        }
        with self.assertRaises(ValidationFailed):
            MaintenanceJob.from_dict({**base, "resource_needs": [{"kind": "crew", "quantity": 1}]})
        job = MaintenanceJob.from_dict(base)
        self.assertEqual([need.kind for need in job.resource_needs],
                         ["mother-vessel", "lifting-window", "spare-kit", "crew"])

    def test_job_rejects_self_dependency_and_duplicates(self) -> None:
        base = {
            "job_id": "j-1", "turbine_id": "t-1", "title": "t",
            "window_start": "2026-10-03T00:00:00Z", "window_end": "2026-10-05T00:00:00Z",
            "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
        }
        with self.assertRaises(ValidationFailed):
            MaintenanceJob.from_dict({**base, "depends_on": ["j-1"]})
        with self.assertRaises(ValidationFailed):
            MaintenanceJob.from_dict({**base, "depends_on": ["j-0", "j-0"]})


class PlanningTests(unittest.TestCase):
    def test_eligibility_failure_explains_each_kind(self) -> None:
        job = _job_spec()
        self.assertIsNone(eligibility_failure(job, _batch_spec("c-a", "crew", qualification="A")))
        self.assertIn("低于最低要求", eligibility_failure(job, _batch_spec("c-c", "crew", qualification="C")))
        self.assertIn("不兼容部件", eligibility_failure(job, _batch_spec("k-1", "spare-kit", compatible_parts=("other",))))
        self.assertIn("抗浪能力", eligibility_failure(job, _batch_spec("v-1", "mother-vessel", max_wave_m=Decimal("1.0"))))
        self.assertIn("未覆盖作业窗口", eligibility_failure(
            job, _batch_spec("v-2", "mother-vessel", window_start=parse_utc("2026-10-04T00:00:00Z"))))

    def test_degradation_rank_prefers_listed_qualifications(self) -> None:
        job = _job_spec()
        self.assertEqual(degradation_rank(job, "B"), 1)
        self.assertEqual(degradation_rank(job, "A"), 2)

    def test_candidates_are_deterministic_and_explain_trade_offs(self) -> None:
        job = _job_spec()
        batches = [
            _batch_spec("vessel-a", "mother-vessel"),
            _batch_spec("crane-a", "lifting-window"),
            _batch_spec("kit-a", "spare-kit"),
            _batch_spec("crew-a", "crew", qualification="A"),
            _batch_spec("crew-b", "crew", qualification="B"),
        ]
        first = generate_candidates([job], batches, {}, 3)
        second = generate_candidates([job], batches, {}, 3)
        self.assertEqual(first, second)
        self.assertEqual(first[0].assignments[-1]["batch_id"], "crew-b")
        self.assertEqual(first[0].assignments[-1]["degradation_rank"], 1)
        degraded = [c for c in first if any(a["batch_id"] == "crew-a" for a in c.assignments)]
        self.assertTrue(degraded)
        self.assertIn("降级", degraded[0].trade_offs[0]["detail"])
        self.assertGreater(degraded[0].score, first[0].score)

    def test_unmet_condition_lists_rejection_reasons(self) -> None:
        job = _job_spec()
        candidates = generate_candidates([job], [_batch_spec("v-1", "mother-vessel", max_wave_m=Decimal("0.5"))], {}, 2)
        kinds = {item["kind"] for item in candidates[0].unmet}
        self.assertEqual(kinds, {"mother-vessel", "lifting-window", "spare-kit", "crew"})
        vessel_reason = next(item["reason"] for item in candidates[0].unmet if item["kind"] == "mother-vessel")
        self.assertIn("v-1", vessel_reason)
        self.assertIn("抗浪能力", vessel_reason)


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = OrchestrationService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("lead", "coordinator"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)

    def tearDown(self) -> None:
        self.connection.close()

    def _batch(self, batch_id: str, kind: str, **extra) -> dict:
        payload = {"batch_id": batch_id, "kind": kind, "name": batch_id, "quantity": 1, **extra}
        if kind in ("mother-vessel", "lifting-window"):
            payload.setdefault("max_wave_m", "2.0")
            payload.setdefault("max_wind_ms", "12")
            payload.setdefault("window_start", "2026-10-01T00:00:00Z")
            payload.setdefault("window_end", "2026-10-10T00:00:00Z")
        if kind == "crew":
            payload.setdefault("qualification", "A")
        if kind == "spare-kit":
            payload.setdefault("compatible_parts", ["blade-bearing-18mw", "yaw-motor-18mw"])
        return self.service.register_batch("plan", payload)

    def _job(self, job_id: str, **extra) -> dict:
        payload = {
            "job_id": job_id, "turbine_id": f"turbine-{job_id}", "title": job_id,
            "window_start": "2026-10-03T00:00:00Z", "window_end": "2026-10-05T00:00:00Z",
            "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
            "required_parts": ["blade-bearing-18mw"],
            "degradation_order": [{"rank": 1, "qualification": "A"}, {"rank": 2, "qualification": "B"}],
            **extra,
        }
        return self.service.register_job("plan", payload)

    def _standard_resources(self, quantity: int = 2) -> None:
        self._batch("vessel-a", "mother-vessel", quantity=quantity)
        self._batch("crane-a", "lifting-window", quantity=quantity)
        self._batch("kit-a", "spare-kit", quantity=quantity)
        self._batch("crew-a", "crew", quantity=quantity)

    def _generate(self, request_id: str, job_ids: list[str], key: str) -> dict:
        return self.service.generate_orchestration("plan", {
            "request_id": request_id, "job_ids": job_ids, "idempotency_key": key,
        })

    def test_permissions_are_enforced(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.register_batch("lead", {"batch_id": "v-1", "kind": "crew", "name": "x", "quantity": 1, "qualification": "A"})
        with self.assertRaises(Forbidden):
            self.service.generate_orchestration("audit", {"request_id": "r", "job_ids": ["j"], "idempotency_key": "k"})
        with self.assertRaises(NotFound):
            self.service.generate_orchestration("nobody", {"request_id": "r", "job_ids": ["j"], "idempotency_key": "k"})
        self._standard_resources()
        self._job("job-1")
        generated = self._generate("req-1", ["job-1"], "key-1")
        with self.assertRaises(Forbidden):
            self.service.confirm_orchestration("plan", "req-1", generated["combinations"][0]["combination_id"], "c-1")

    def test_generate_is_idempotent_and_rejects_changed_payload(self) -> None:
        self._standard_resources()
        self._job("job-1")
        payload = {"request_id": "req-1", "job_ids": ["job-1"], "idempotency_key": "key-1"}
        first = self.service.generate_orchestration("plan", payload)
        second = self.service.generate_orchestration("plan", payload)
        self.assertEqual(first, second)
        self.assertEqual(first["state"], "generated")
        self.assertTrue(all(item["revision"] == 1 for item in first["snapshot"]))
        with self.assertRaises(Conflict):
            self.service.generate_orchestration("plan", {**payload, "job_ids": ["job-1", "job-2"]})
        with self.assertRaises(Conflict):
            self.service.generate_orchestration("plan", {"request_id": "req-1", "job_ids": ["job-1"], "idempotency_key": "other-key"})

    def test_dependency_must_be_secured_before_confirm(self) -> None:
        self._standard_resources()
        self._job("job-1")
        self._job("job-2", depends_on=["job-1"])
        generated = self._generate("req-1", ["job-2"], "key-1")
        unmet = generated["combinations"][0]["unmet_conditions"]
        self.assertEqual(unmet[0]["kind"], "dependency")
        self.assertIn("job-1", unmet[0]["reason"])
        with self.assertRaises(Conflict):
            self.service.confirm_orchestration("lead", "req-1", generated["combinations"][0]["combination_id"], "c-1")
        both = self._generate("req-2", ["job-1", "job-2"], "key-2")
        self.assertEqual(both["combinations"][0]["unmet_conditions"], [])

    def test_confirm_occupies_atomically_and_replay_does_not_double_occupy(self) -> None:
        self._standard_resources(quantity=2)
        self._job("job-1")
        self._job("job-2")
        generated = self._generate("req-1", ["job-1", "job-2"], "key-1")
        combination_id = generated["combinations"][0]["combination_id"]
        confirmed = self.service.confirm_orchestration("lead", "req-1", combination_id, "confirm-1")
        self.assertEqual(confirmed["state"], "confirmed")
        self.assertEqual(len(confirmed["reservations"]), 8)
        self.assertEqual(self.service.batch("plan", "vessel-a")["available"], 0)
        replayed = self.service.confirm_orchestration("lead", "req-1", combination_id, "confirm-1")
        self.assertEqual(replayed, confirmed)
        self.assertEqual(self.service.batch("plan", "vessel-a")["available"], 0)
        with self.assertRaises(Conflict):
            self.service.confirm_orchestration("lead", "req-other", combination_id, "confirm-1")
        with self.assertRaises(InvalidState):
            self.service.confirm_orchestration("lead", "req-1", combination_id, "confirm-2")
        job = self.service.job("plan", "job-1")
        self.assertEqual(job["state"], "confirmed")
        self.assertEqual({item["batch_id"] for item in job["reservations"]},
                         {"vessel-a", "crane-a", "kit-a", "crew-a"})
        self.assertTrue(all(item["batch_revision"] == 1 for item in job["reservations"]))

    def test_confirm_fails_when_any_batch_version_changed(self) -> None:
        self._standard_resources(quantity=2)
        self._job("job-1")
        generated = self._generate("req-1", ["job-1"], "key-1")
        combination_id = generated["combinations"][0]["combination_id"]
        self.service.set_batch_state("plan", "crew-a", "suspended", 1)
        with self.assertRaises(Conflict):
            self.service.confirm_orchestration("lead", "req-1", combination_id, "confirm-1")
        # 整单失败：吊装窗口等其它批次不得被部分占用
        self.assertEqual(self.service.batch("plan", "crane-a")["available"], 2)
        self.assertEqual(self.service.batch("plan", "crane-a")["revision"], 1)
        self.assertEqual(self.service.orchestration("plan", "req-1")["state"], "generated")
        self.service.set_batch_state("plan", "crew-a", "active", 2)
        regenerated = self._generate("req-2", ["job-1"], "key-2")
        confirmed = self.service.confirm_orchestration(
            "lead", "req-2", regenerated["combinations"][0]["combination_id"], "confirm-2")
        self.assertEqual(confirmed["state"], "confirmed")

    def test_confirm_rolls_back_when_later_batch_conflicts(self) -> None:
        self._standard_resources(quantity=2)
        self._job("job-1")
        self._job("job-2")
        first = self._generate("req-1", ["job-1"], "key-1")
        second = self._generate("req-2", ["job-2"], "key-2")
        self.service.confirm_orchestration("lead", "req-2", second["combinations"][0]["combination_id"], "c-2")
        with self.assertRaises(Conflict):
            self.service.confirm_orchestration("lead", "req-1", first["combinations"][0]["combination_id"], "c-1")
        # req-2 已占一单位，req-1 整单失败不得再占
        self.assertEqual(self.service.batch("plan", "vessel-a")["available"], 1)
        self.assertEqual(self.service.job("plan", "job-1")["state"], "planned")

    def test_started_job_cannot_be_reorchestrated(self) -> None:
        self._standard_resources()
        self._job("job-1")
        generated = self._generate("req-1", ["job-1"], "key-1")
        self.service.confirm_orchestration("lead", "req-1", generated["combinations"][0]["combination_id"], "c-1")
        started = self.service.start_job("lead", "job-1", 2)
        self.assertEqual(started["consumed_reservations"], 4)
        with self.assertRaises(InvalidState):
            self._generate("req-2", ["job-1"], "key-2")
        with self.assertRaises(InvalidState):
            self.service.start_job("lead", "job-1", 3)
        completed = self.service.complete_job("lead", "job-1", 3)
        self.assertEqual(completed["state"], "completed")

    def test_cancel_releases_only_unconsumed_reservations(self) -> None:
        self._standard_resources(quantity=2)
        self._job("job-1")
        self._job("job-2")
        generated = self._generate("req-1", ["job-1", "job-2"], "key-1")
        self.service.confirm_orchestration("lead", "req-1", generated["combinations"][0]["combination_id"], "c-1")
        self.service.start_job("lead", "job-1", 2)
        cancelled = self.service.cancel_orchestration("lead", "req-1", "吊装船临时故障")
        self.assertEqual(len(cancelled["released_reservations"]), 4)
        self.assertEqual(len(cancelled["retained_consumed_reservations"]), 4)
        self.assertEqual(self.service.batch("plan", "vessel-a")["available"], 1)
        self.assertEqual(self.service.job("plan", "job-1")["state"], "started")
        self.assertEqual(self.service.job("plan", "job-2")["state"], "cancelled")
        job_2 = self.service.job("plan", "job-2")
        self.assertTrue(all(item["state"] == "released" for item in job_2["reservations"]))
        with self.assertRaises(InvalidState):
            self.service.cancel_orchestration("lead", "req-1", "重复取消")

    def test_degradation_trade_off_and_unmet_traceability(self) -> None:
        self._batch("vessel-a", "mother-vessel", quantity=2)
        self._batch("crane-a", "lifting-window", quantity=2)
        self._batch("kit-a", "spare-kit", quantity=2)
        self._batch("crew-a", "crew", quantity=1, qualification="A")
        self._batch("crew-b", "crew", quantity=1, qualification="B")
        self._job("job-1")
        self._job("job-2")
        generated = self._generate("req-1", ["job-1", "job-2"], "key-1")
        preferred = generated["combinations"][0]
        crews = {item["job_id"]: item["batch_id"] for item in preferred["assignments"] if item["kind"] == "crew"}
        self.assertEqual(crews, {"job-1": "crew-a", "job-2": "crew-b"})
        self.assertTrue(any("降级" in item["detail"] for item in preferred["trade_offs"]))
        self._job("job-3", required_parts=["gearbox-18mw"])
        blocked = self._generate("req-2", ["job-3"], "key-2")
        unmet = blocked["combinations"][0]["unmet_conditions"]
        self.assertEqual(unmet[0]["kind"], "spare-kit")
        self.assertIn("gearbox-18mw", unmet[0]["reason"])
        job_3 = self.service.job("plan", "job-3")
        self.assertEqual(job_3["unmet_conditions"], unmet)
        with self.assertRaises(Conflict):
            self.service.confirm_orchestration("lead", "req-2", blocked["combinations"][0]["combination_id"], "c-2")

    def test_suspended_batch_is_excluded_from_candidates(self) -> None:
        self._standard_resources()
        self._job("job-1")
        self.service.set_batch_state("plan", "vessel-a", "suspended", 1)
        generated = self._generate("req-1", ["job-1"], "key-1")
        unmet = generated["combinations"][0]["unmet_conditions"]
        self.assertEqual([item["kind"] for item in unmet], ["mother-vessel"])
        with self.assertRaises(Conflict):
            self.service.set_batch_state("plan", "vessel-a", "active", 1)
        self.service.set_batch_state("plan", "vessel-a", "active", 2)
        self.service.set_batch_state("plan", "vessel-a", "retired", 3)
        with self.assertRaises(InvalidState):
            self.service.set_batch_state("plan", "vessel-a", "active", 4)

    def test_cancel_generated_request_holds_no_resources(self) -> None:
        self._standard_resources()
        self._job("job-1")
        self._generate("req-1", ["job-1"], "key-1")
        cancelled = self.service.cancel_orchestration("lead", "req-1", "计划调整")
        self.assertEqual(cancelled["released_reservations"], [])
        self.assertEqual(self.service.batch("plan", "vessel-a")["available"], 2)
        self.assertEqual(self.service.job("plan", "job-1")["state"], "planned")
        with self.assertRaises(InvalidState):
            self.service.confirm_orchestration("lead", "req-1", 1, "c-1")

    def test_register_job_validates_dependencies(self) -> None:
        with self.assertRaises(ValidationFailed):
            self._job("job-1", depends_on=["missing"])
        self._job("job-1")
        with self.assertRaises(Conflict):
            self._job("job-1")

    def test_audit_chain_detects_tampering(self) -> None:
        self._standard_resources()
        self._job("job-1")
        self._generate("req-1", ["job-1"], "key-1")
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE mo_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.service = OrchestrationService(self.connection)
        self.app = JsonApplication(self.service)

    def tearDown(self) -> None:
        self.connection.close()

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")

    def test_json_error_shape(self) -> None:
        response = self.app.handle("POST", "/users", body=b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")

    def test_missing_actor_is_rejected(self) -> None:
        response = self.app.handle("POST", "/users", body=b"{}")
        self.assertEqual(response.status, 422)
        self.assertIn("X-Actor-Id", response.body["error"]["message"])

    def test_orchestration_flow_over_http(self) -> None:
        planner = {"X-Actor-Id": "plan"}
        lead = {"X-Actor-Id": "lead"}
        self.service.create_user("plan", "计划", "planner")
        self.service.create_user("lead", "运维", "coordinator")
        for kind, extra in (
            ("mother-vessel", {"max_wave_m": "2", "max_wind_ms": "12", "window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-10T00:00:00Z"}),
            ("lifting-window", {"max_wave_m": "2", "max_wind_ms": "12", "window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-10T00:00:00Z"}),
            ("spare-kit", {"compatible_parts": ["blade-bearing-18mw"]}),
            ("crew", {"qualification": "A"}),
        ):
            response = self.app.handle("POST", "/batches", planner,
                                       json.dumps({"batch_id": kind, "kind": kind, "name": kind, "quantity": 1, **extra}).encode())
            self.assertEqual(response.status, 201)
        response = self.app.handle("POST", "/jobs", planner, json.dumps({
            "job_id": "job-1", "turbine_id": "t-1", "title": "检修",
            "window_start": "2026-10-03T00:00:00Z", "window_end": "2026-10-05T00:00:00Z",
            "max_wave_m": "1.5", "max_wind_ms": "10", "min_qualification": "B",
        }).encode())
        self.assertEqual(response.status, 201)
        response = self.app.handle("POST", "/orchestrations", planner, json.dumps({
            "request_id": "req-1", "job_ids": ["job-1"], "idempotency_key": "k-1",
        }).encode())
        self.assertEqual(response.status, 201)
        combination_id = response.body["combinations"][0]["combination_id"]
        response = self.app.handle("POST", "/orchestrations/req-1/confirm", lead,
                                   json.dumps({"combination_id": combination_id, "idempotency_key": "c-1"}).encode())
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["state"], "confirmed")
        response = self.app.handle("GET", "/jobs/job-1", lead)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["state"], "confirmed")
        self.assertEqual(len(response.body["reservations"]), 4)
        response = self.app.handle("GET", "/orchestrations/req-1", lead)
        self.assertEqual(response.body["state"], "confirmed")
        response = self.app.handle("GET", "/unknown", lead)
        self.assertEqual(response.status, 404)


if __name__ == "__main__":
    unittest.main()
