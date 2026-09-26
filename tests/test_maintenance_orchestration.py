from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone
from dataclasses import replace
from decimal import Decimal

from maintenance_orchestration.api import JsonApplication
from maintenance_orchestration.clock import FrozenClock
from maintenance_orchestration.contracts import MaintenanceOperation
from maintenance_orchestration.errors import Conflict, Forbidden, InvalidState, ValidationFailed
from maintenance_orchestration.orchestration import (
    ResourceSnapshot,
    plan_candidates,
    windows_overlap,
)
from maintenance_orchestration.service import MaintenanceOrchestrationService


VESSEL_A = {"resource_id": "vessel-a", "kind": "vessel", "batch_id": "B-VA",
            "capability_mw": "20", "sea_state_max_m": "2.5"}
VESSEL_B = {"resource_id": "vessel-b", "kind": "vessel", "batch_id": "B-VB",
            "capability_mw": "18", "sea_state_max_m": "1.8"}
WIN_AM = {"resource_id": "win-am", "kind": "lifting_window", "batch_id": "B-WA", "capability_mw": "20"}
KIT_A = {"resource_id": "kit-a", "kind": "spare_kit", "batch_id": "B-KA", "compat_models": ["W-18", "W-16"]}
KIT_B = {"resource_id": "kit-b", "kind": "spare_kit", "batch_id": "B-KB", "compat_models": ["W-16"]}
CREW_A = {"resource_id": "crew-a", "kind": "crew", "batch_id": "B-CA", "certifications": ["HOIST", "GWO"]}
CREW_B = {"resource_id": "crew-b", "kind": "crew", "batch_id": "B-CB", "certifications": ["GWO"]}

WINDOW = ("2026-09-28T02:00:00Z", "2026-09-28T08:00:00Z")


def operation_payload(operation_id: str = "op-1", *, deps=None, essential_crew: bool = True) -> dict:
    prefix = operation_id + "-"
    requirements = [
        {"requirement_id": prefix + "v", "kind": "vessel", "minimum_capability_mw": "18",
         "sea_state_limit_m": "2.0", "preference_order": ["vessel-a", "vessel-b"]},
        {"requirement_id": prefix + "w", "kind": "lifting_window", "minimum_capability_mw": "18",
         "preference_order": ["win-am"]},
        {"requirement_id": prefix + "k", "kind": "spare_kit", "required_model": "W-18",
         "preference_order": ["kit-a", "kit-b"]},
        {"requirement_id": prefix + "c", "kind": "crew", "minimum_certification": "HOIST",
         "preference_order": ["crew-a", "crew-b"], "essential": essential_crew},
    ]
    return {
        "operation_id": operation_id,
        "turbine_model": "W-18",
        "depends_on": deps or [],
        "sea_window": {"starts_at": WINDOW[0], "ends_at": WINDOW[1]},
        "requirements": requirements,
    }


class EngineTests(unittest.TestCase):
    def _snapshots(self) -> dict[str, ResourceSnapshot]:
        rows = {
            "vessel-a": ("vessel", "B-VA", "20", "2.5", [], [], 1),
            "vessel-b": ("vessel", "B-VB", "18", "1.8", [], [], 1),
            "win-am": ("lifting_window", "B-WA", "20", None, [], [], 2),
            "kit-a": ("spare_kit", "B-KA", None, None, [], ["W-18", "W-16"], 2),
            "kit-b": ("spare_kit", "B-KB", None, None, [], ["W-16"], 2),
            "crew-a": ("crew", "B-CA", None, None, ["HOIST", "GWO"], [], 2),
            "crew-b": ("crew", "B-CB", None, None, ["GWO"], [], 2),
        }
        snapshots: dict[str, ResourceSnapshot] = {}
        for resource_id, (kind, batch, cap, sea, certs, models, capacity) in rows.items():
            snapshots[resource_id] = ResourceSnapshot(
                resource_id=resource_id, kind=kind, batch_id=batch,
                capability_mw=None if cap is None else Decimal(cap),
                sea_state_max_m=None if sea is None else Decimal(sea),
                available_from=None, available_to=None,
                certifications=frozenset(certs), compat_models=frozenset(models),
                capacity=capacity, revision=1, active=True,
            )
        return snapshots

    def test_eligibility_explains_every_unmet_condition(self) -> None:
        operation = MaintenanceOperation.from_dict(operation_payload())
        result = plan_candidates([operation], self._snapshots(), {})
        view = {item["kind"]: item for item in result["operations"][0]["requirements"]}
        vessel_b = next(item for item in view["vessel"]["evaluation"] if item["resource_id"] == "vessel-b")
        self.assertFalse(vessel_b["eligible"])
        self.assertIn("sea_state_exceeded", {reason["code"] for reason in vessel_b["unmet_conditions"]})
        kit_b = next(item for item in view["spare_kit"]["evaluation"] if item["resource_id"] == "kit-b")
        self.assertIn("model_incompatible", {reason["code"] for reason in kit_b["unmet_conditions"]})
        crew_b = next(item for item in view["crew"]["evaluation"] if item["resource_id"] == "crew-b")
        self.assertIn("certification_missing", {reason["code"] for reason in crew_b["unmet_conditions"]})

    def test_capacity_conflict_marks_preferred_combo_infeasible_and_degrades(self) -> None:
        snapshots = self._snapshots()
        # 替代母船浪高达标后，两作业可分别使用主力与替代母船。
        snapshots["vessel-b"] = replace(snapshots["vessel-b"], sea_state_max_m=Decimal("2.5"))
        first = MaintenanceOperation.from_dict(operation_payload("op-a"))
        second = MaintenanceOperation.from_dict(operation_payload("op-b"))
        result = plan_candidates([first, second], snapshots, {})
        self.assertTrue(result["feasible"])
        infeasible = [item for item in result["candidates"] if not item["feasible"]]
        self.assertTrue(infeasible)
        self.assertTrue(any("vessel-a" in conflict for conflict in infeasible[0]["conflicts"]))
        best = next(item for item in result["candidates"] if item["feasible"])
        vessels = {
            chosen["resource_id"]
            for operation_choice in best["selections"].values()
            for chosen in operation_choice.values()
            if chosen["kind"] == "vessel"
        }
        self.assertEqual(vessels, {"vessel-a", "vessel-b"})

    def test_windows_overlap_predicate(self) -> None:
        self.assertTrue(windows_overlap(("t1", "t3"), ("t2", "t4")))
        self.assertFalse(windows_overlap(("t1", "t2"), ("t2", "t3")))

    def test_nonessential_requirement_can_be_omitted(self) -> None:
        # 高级班组停用后，唯一不达标偏好下非必要班组环节可整体省略。
        snapshots = self._snapshots()
        snapshots["crew-a"] = replace(snapshots["crew-a"], active=False)
        operation = MaintenanceOperation.from_dict(operation_payload(essential_crew=True))
        result = plan_candidates([operation], snapshots, {})
        self.assertFalse(result["feasible"])
        omitted_operation = MaintenanceOperation.from_dict(operation_payload(essential_crew=False))
        result_omitted = plan_candidates([omitted_operation], snapshots, {})
        self.assertTrue(result_omitted["feasible"])
        self.assertTrue(result_omitted["candidates"][0]["omitted"])


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        self.service = MaintenanceOrchestrationService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"),
                              ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        for resource in (VESSEL_A, VESSEL_B, WIN_AM, KIT_A, KIT_B, CREW_A, CREW_B):
            self.service.register_resource("plan", resource)

    def tearDown(self) -> None:
        self.connection.close()

    def _plan_with_one_operation(self, plan_id: str = "plan-1", operation_id: str = "op-1") -> int:
        self.service.register_operation("plan", operation_payload(operation_id))
        self.service.freeze_plan("plan", {"plan_id": plan_id, "operation_ids": [operation_id]})
        return self.service.generate_candidates("plan", plan_id)["run_id"]

    def test_roles_are_enforced(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.register_resource("audit", VESSEL_A)
        with self.assertRaises(Forbidden):
            self.service.freeze_plan("dispatch", {"plan_id": "p", "operation_ids": []})

    def test_registration_validates_compat_and_certification_scope(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.register_resource("plan", {
                "resource_id": "bad-vessel", "kind": "vessel", "batch_id": "B",
                "capability_mw": "20", "certifications": ["HOIST"]})
        with self.assertRaises(ValidationFailed):
            self.service.register_resource("plan", {
                "resource_id": "bad-kit", "kind": "spare_kit", "batch_id": "B",
                "compat_models": []})
        with self.assertRaises(ValidationFailed):
            self.service.register_operation("plan", operation_payload() | {
                "sea_window": {"starts_at": WINDOW[1], "ends_at": WINDOW[0]}})

    def test_dependency_must_exist_and_freeze_in_dependency_order(self) -> None:
        payload = operation_payload("op-x", deps=["missing"])
        with self.assertRaises(ValidationFailed):
            self.service.register_operation("plan", payload)
        self.service.register_operation("plan", operation_payload("op-base"))
        self.service.register_operation("plan", operation_payload("op-top", deps=["op-base"]))
        with self.assertRaises(ValidationFailed):
            self.service.freeze_plan("plan", {"plan_id": "p", "operation_ids": ["op-top", "op-base"]})

    def test_identical_frozen_content_is_deduplicated(self) -> None:
        run_id = self._plan_with_one_operation("plan-1")
        self.assertEqual(run_id, 1)
        # 同一冻结内容（同一组作业定义）不能用不同计划编号重复冻结。
        with self.assertRaises(Conflict):
            self.service.freeze_plan("plan", {"plan_id": "plan-2", "operation_ids": ["op-1"]})

    def test_candidate_run_is_replayed_deterministically(self) -> None:
        run_id = self._plan_with_one_operation()
        first = self.service.generate_candidates("plan", "plan-1")
        second = self.service.generate_candidates("plan", "plan-1")
        self.assertTrue(second["replayed"])
        self.assertEqual(first["run_id"], second["run_id"])

    def test_confirm_is_idempotent_and_rejects_different_payload(self) -> None:
        run_id = self._plan_with_one_operation()
        request = {"plan_id": "plan-1", "order_id": "ord-1", "candidate_run_id": run_id,
                   "candidate_ordinal": 1, "idempotency_key": "key-1"}
        first = self.service.confirm_order("dispatch", request)
        second = self.service.confirm_order("dispatch", request)
        self.assertEqual(first, second)
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM resource_reservations").fetchone()[0], 4)
        changed = dict(request, candidate_ordinal=2)
        with self.assertRaises(Conflict):
            self.service.confirm_order("dispatch", changed)

    def test_resource_revision_change_fails_entire_confirmation(self) -> None:
        run_id = self._plan_with_one_operation()
        self.service.update_resource("plan", "vessel-a", {"note": "临时不可用"})
        with self.assertRaises(Conflict):
            self.service.confirm_order("dispatch", {
                "plan_id": "plan-1", "order_id": "ord-stale", "candidate_run_id": run_id,
                "candidate_ordinal": 1, "idempotency_key": "key-stale"})
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM resource_reservations").fetchone()[0], 0)

    def test_fresh_run_after_revision_confirms(self) -> None:
        run_id = self._plan_with_one_operation()
        self.service.update_resource("plan", "vessel-a", {"note": "临时不可用"})
        fresh = self.service.generate_candidates("plan", "plan-1")
        self.assertNotEqual(run_id, fresh["run_id"])
        order = self.service.confirm_order("dispatch", {
            "plan_id": "plan-1", "order_id": "ord-fresh", "candidate_run_id": fresh["run_id"],
            "candidate_ordinal": 1, "idempotency_key": "key-fresh"})
        self.assertEqual(order["state"], "confirmed")

    def test_started_operation_is_pinned_and_cancel_keeps_consumed(self) -> None:
        run_id = self._plan_with_one_operation()
        order = self.service.confirm_order("dispatch", {
            "plan_id": "plan-1", "order_id": "ord-1", "candidate_run_id": run_id,
            "candidate_ordinal": 1, "idempotency_key": "key-1"})
        self.service.start_operation("dispatch", "op-1")
        run = self.service.generate_candidates("plan", "plan-1")
        self.assertEqual([item["operation_id"] for item in run["pinned_operations"]], ["op-1"])
        # 已开工单的唯一候选仍固定原资源，不能在没有可确认新预留时重复成单。
        with self.assertRaises(InvalidState):
            self.service.confirm_order("dispatch", {
                "plan_id": "plan-1", "order_id": "ord-dup", "candidate_run_id": run["run_id"],
                "candidate_ordinal": 1, "idempotency_key": "key-dup"})
        cancelled = self.service.cancel_order("dispatch", "ord-1", "窗口取消")
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertTrue(all(item["state"] == "consumed" for item in cancelled["reservations"]))
        with self.assertRaises(InvalidState):
            self.service.cancel_order("dispatch", "ord-1", "再次取消")

    def test_cancel_before_start_releases_reserved(self) -> None:
        run_id = self._plan_with_one_operation()
        self.service.confirm_order("dispatch", {
            "plan_id": "plan-1", "order_id": "ord-1", "candidate_run_id": run_id,
            "candidate_ordinal": 1, "idempotency_key": "key-1"})
        cancelled = self.service.cancel_order("dispatch", "ord-1", "计划调整")
        self.assertTrue(all(item["state"] == "released" for item in cancelled["reservations"]))
        # 释放后可重新确认同一作业。
        fresh = self.service.generate_candidates("plan", "plan-1")
        reordered = self.service.confirm_order("dispatch", {
            "plan_id": "plan-1", "order_id": "ord-2", "candidate_run_id": fresh["run_id"],
            "candidate_ordinal": 1, "idempotency_key": "key-2"})
        self.assertEqual(len(reordered["reservations"]), 4)

    def test_reservation_traces_batch_and_revision(self) -> None:
        run_id = self._plan_with_one_operation()
        order = self.service.confirm_order("dispatch", {
            "plan_id": "plan-1", "order_id": "ord-1", "candidate_run_id": run_id,
            "candidate_ordinal": 1, "idempotency_key": "key-1"})
        by_kind = {item["kind"]: item for item in order["reservations"]}
        self.assertEqual(by_kind["vessel"]["batch_id"], "B-VA")
        self.assertEqual(by_kind["spare_kit"]["batch_id"], "B-KA")
        self.assertEqual(by_kind["crew"]["resource_revision"], 1)

    def test_audit_chain_detects_tampering(self) -> None:
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE mo_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.service = MaintenanceOrchestrationService(self.connection)
        self.app = JsonApplication(self.service)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"),
                              ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)

    def tearDown(self) -> None:
        self.connection.close()

    def test_health_and_route_boundary(self) -> None:
        self.assertEqual(self.app.handle("GET", "/health").status, 200)
        response = self.app.handle("GET", "/resources/nope", {"X-Actor-Id": "plan"})
        self.assertEqual(response.status, 404)
        self.assertEqual(response.body["error"]["code"], "not_found")

    def test_permission_denied_via_http(self) -> None:
        response = self.app.handle("POST", "/resources", {"X-Actor-Id": "audit"},
                                   b'{"resource_id":"x","kind":"vessel","batch_id":"b","capability_mw":"1"}')
        self.assertEqual(response.status, 403)
        self.assertEqual(response.body["error"]["code"], "forbidden")


if __name__ == "__main__":
    unittest.main()
