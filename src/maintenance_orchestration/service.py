"""检修资源批次、作业登记、候选编排和原子确认的事务用例。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from typing import Any, Mapping

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import (
    BATCH_STATES,
    RESOURCE_KINDS,
    MaintenanceJob,
    OrchestrationRequestInput,
    ResourceBatch,
    DegradationStep,
    ResourceNeed,
)
from .planning import (
    BatchSpec,
    JobSpec,
    canonical_json,
    decimal_text,
    digest,
    generate_candidates,
)
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"batch.write", "job.write", "orchestration.run", "orchestration.read"},
    "coordinator": {
        "orchestration.confirm", "orchestration.cancel", "job.start", "job.complete",
        "orchestration.read",
    },
    "auditor": {"orchestration.read", "audit.read"},
}


def _job_spec(row: sqlite3.Row) -> JobSpec:
    degradation = tuple(
        DegradationStep(int(step["rank"]), step["qualification"], step.get("note", ""))
        for step in json.loads(row["degradation_json"])
    )
    needs = tuple(
        ResourceNeed(need["kind"], int(need["quantity"]))
        for need in json.loads(row["resource_needs_json"])
    )
    return JobSpec(
        job_id=row["job_id"],
        window_start=parse_utc(row["window_start"]),
        window_end=parse_utc(row["window_end"]),
        max_wave_m=Decimal(row["max_wave_m"]),
        max_wind_ms=Decimal(row["max_wind_ms"]),
        min_qualification=row["min_qualification"],
        required_parts=tuple(json.loads(row["required_parts_json"])),
        degradation=degradation,
        needs=needs,
    )


def _batch_spec(row: sqlite3.Row) -> BatchSpec:
    return BatchSpec(
        batch_id=row["batch_id"],
        kind=row["kind"],
        revision=int(row["revision"]),
        available=int(row["available"]),
        qualification=row["qualification"],
        compatible_parts=tuple(json.loads(row["compatible_parts_json"])) if row["compatible_parts_json"] else (),
        max_wave_m=Decimal(row["max_wave_m"]) if row["max_wave_m"] else None,
        max_wind_ms=Decimal(row["max_wind_ms"]) if row["max_wind_ms"] else None,
        window_start=parse_utc(row["window_start"]) if row["window_start"] else None,
        window_end=parse_utc(row["window_end"]) if row["window_end"] else None,
    )


class OrchestrationService:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM mo_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound("用户不存在")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM mo_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO mo_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO mo_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    def register_batch(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "batch.write")
        batch = ResourceBatch.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO resource_batches(batch_id,kind,name,qualification,compatible_parts_json,"
                    "max_wave_m,max_wind_ms,window_start,window_end,quantity,available,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        batch.batch_id,
                        batch.kind,
                        batch.name,
                        batch.qualification,
                        canonical_json(list(batch.compatible_parts)) if batch.compatible_parts else None,
                        None if batch.max_wave_m is None else decimal_text(batch.max_wave_m),
                        None if batch.max_wind_ms is None else decimal_text(batch.max_wind_ms),
                        batch.window_start,
                        batch.window_end,
                        batch.quantity,
                        batch.quantity,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("batch", batch.batch_id, "batch.registered", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("资源批次编号已经存在") from exc
        return self.batch(actor_id, batch.batch_id)

    def set_batch_state(self, actor_id: str, batch_id: str, state: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "batch.write")
        if state not in BATCH_STATES:
            raise ValidationFailed("state 必须是 active、suspended 或 retired")
        row = self.connection.execute(
            "SELECT state, revision FROM resource_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if row is None:
            raise NotFound("资源批次不存在")
        if row["state"] == "retired":
            raise InvalidState("资源批次已退役，不能变更状态")
        if row["state"] == state:
            raise InvalidState(f"资源批次已处于 {state} 状态")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE resource_batches SET state=?,revision=revision+1 WHERE batch_id=? AND revision=?",
                (state, batch_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise Conflict("资源批次版本已变化，状态变更失败")
            self._audit(
                "batch", batch_id, "batch.state_changed", actor_id,
                {"from": row["state"], "to": state},
            )
        return self.batch(actor_id, batch_id)

    def batch(self, actor_id: str, batch_id: str) -> dict[str, Any]:
        self._require(actor_id, "orchestration.read")
        row = self.connection.execute(
            "SELECT * FROM resource_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if row is None:
            raise NotFound("资源批次不存在")
        result = dict(row)
        result["compatible_parts"] = (
            json.loads(row["compatible_parts_json"]) if row["compatible_parts_json"] else []
        )
        del result["compatible_parts_json"]
        result["reservations"] = [
            dict(item)
            for item in self.connection.execute(
                "SELECT reservation_id,request_id,combination_id,job_id,kind,batch_revision,quantity,state,"
                "created_at,consumed_at,released_at FROM resource_reservations WHERE batch_id=? "
                "ORDER BY reservation_id",
                (batch_id,),
            ).fetchall()
        ]
        return result

    def register_job(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "job.write")
        job = MaintenanceJob.from_dict(raw)
        for dependency in job.depends_on:
            row = self.connection.execute(
                "SELECT state FROM maintenance_jobs WHERE job_id=?", (dependency,)
            ).fetchone()
            if row is None:
                raise ValidationFailed(f"依赖作业不存在: {dependency}")
            if row["state"] == "cancelled":
                raise ValidationFailed(f"依赖作业已取消: {dependency}")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO maintenance_jobs(job_id,turbine_id,title,depends_on_json,window_start,window_end,"
                    "max_wave_m,max_wind_ms,min_qualification,required_parts_json,degradation_json,"
                    "resource_needs_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        job.job_id,
                        job.turbine_id,
                        job.title,
                        canonical_json(list(job.depends_on)),
                        job.window_start,
                        job.window_end,
                        decimal_text(job.max_wave_m),
                        decimal_text(job.max_wind_ms),
                        job.min_qualification,
                        canonical_json(list(job.required_parts)),
                        canonical_json([
                            {"rank": step.rank, "qualification": step.qualification, "note": step.note}
                            for step in job.degradation
                        ]),
                        canonical_json([
                            {"kind": need.kind, "quantity": need.quantity} for need in job.resource_needs
                        ]),
                        actor_id,
                        self._now(),
                    ),
                )
                for dependency in job.depends_on:
                    self.connection.execute(
                        "INSERT INTO job_dependencies(job_id,depends_on_job_id) VALUES(?,?)",
                        (job.job_id, dependency),
                    )
                self._audit("job", job.job_id, "job.registered", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("作业编号已经存在") from exc
        return self.job(actor_id, job.job_id)

    def _job_row(self, job_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM maintenance_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None:
            raise NotFound("检修作业不存在")
        return row

    def _latest_unmet(self, job_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT assignments_json,unmet_json FROM orchestration_combinations "
            "ORDER BY combination_id DESC"
        ).fetchall()
        for row in rows:
            assignments = json.loads(row["assignments_json"])
            unmet = json.loads(row["unmet_json"])
            if any(item["job_id"] == job_id for item in assignments) or any(
                item["job_id"] == job_id for item in unmet
            ):
                return [item for item in unmet if item["job_id"] == job_id]
        return []

    def job(self, actor_id: str, job_id: str) -> dict[str, Any]:
        self._require(actor_id, "orchestration.read")
        row = self._job_row(job_id)
        result = dict(row)
        depends_on = json.loads(row["depends_on_json"])
        result["depends_on"] = depends_on
        result["required_parts"] = json.loads(row["required_parts_json"])
        result["degradation_order"] = json.loads(row["degradation_json"])
        result["resource_needs"] = json.loads(row["resource_needs_json"])
        for key in ("depends_on_json", "required_parts_json", "degradation_json", "resource_needs_json"):
            del result[key]
        dependencies = []
        for dependency in depends_on:
            dep_row = self.connection.execute(
                "SELECT state FROM maintenance_jobs WHERE job_id=?", (dependency,)
            ).fetchone()
            dependencies.append({"job_id": dependency, "state": None if dep_row is None else dep_row["state"]})
        result["dependencies"] = dependencies
        result["reservations"] = [
            dict(item)
            for item in self.connection.execute(
                "SELECT reservation_id,request_id,combination_id,kind,batch_id,batch_revision,quantity,state,"
                "created_at,consumed_at,released_at FROM resource_reservations WHERE job_id=? "
                "ORDER BY reservation_id",
                (job_id,),
            ).fetchall()
        ]
        result["unmet_conditions"] = self._latest_unmet(job_id)
        return result

    def generate_orchestration(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "orchestration.run")
        request = OrchestrationRequestInput.from_dict(raw)
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM mo_idempotency "
            "WHERE scope='orchestration-generate' AND idempotency_key=?",
            (request.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同编排内容")
            return json.loads(stored["response_json"])
        job_rows = []
        for job_id in request.job_ids:
            row = self._job_row(job_id)
            if row["state"] != "planned":
                raise InvalidState(
                    f"作业 {job_id} 当前状态为 {row['state']}，已锁定或已开工的作业不能纳入自动编排"
                )
            job_rows.append(row)
        requested = set(request.job_ids)
        dependency_unmet: dict[str, tuple[str, ...]] = {}
        for row in job_rows:
            reasons = []
            for dependency in json.loads(row["depends_on_json"]):
                if dependency in requested:
                    continue
                dep_row = self.connection.execute(
                    "SELECT state FROM maintenance_jobs WHERE job_id=?", (dependency,)
                ).fetchone()
                if dep_row["state"] not in ("confirmed", "started", "completed"):
                    reasons.append(f"依赖作业 {dependency} 尚未锁定资源（当前状态 {dep_row['state']}）")
            if reasons:
                dependency_unmet[row["job_id"]] = tuple(reasons)
        batch_rows = self.connection.execute(
            "SELECT * FROM resource_batches WHERE state='active' ORDER BY batch_id"
        ).fetchall()
        snapshot = [
            {"batch_id": row["batch_id"], "revision": row["revision"], "available": row["available"]}
            for row in batch_rows
        ]
        candidates = generate_candidates(
            [_job_spec(row) for row in job_rows],
            [_batch_spec(row) for row in batch_rows],
            dependency_unmet,
            request.max_candidates,
        )
        frozen_at = self._now()
        combinations: list[dict[str, Any]] = []
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO orchestration_requests(request_id,idempotency_key,job_ids_json,snapshot_json,"
                    "frozen_at,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        request.request_id,
                        request.idempotency_key,
                        canonical_json(list(request.job_ids)),
                        canonical_json(snapshot),
                        frozen_at,
                        actor_id,
                        frozen_at,
                    ),
                )
                for rank, candidate in enumerate(candidates, start=1):
                    cursor = self.connection.execute(
                        "INSERT INTO orchestration_combinations(request_id,rank,score,assignments_json,"
                        "trade_offs_json,unmet_json) VALUES(?,?,?,?,?,?)",
                        (
                            request.request_id,
                            rank,
                            candidate.score,
                            canonical_json(list(candidate.assignments)),
                            canonical_json(list(candidate.trade_offs)),
                            canonical_json(list(candidate.unmet)),
                        ),
                    )
                    combinations.append({
                        "combination_id": int(cursor.lastrowid),
                        "rank": rank,
                        "score": candidate.score,
                        "assignments": list(candidate.assignments),
                        "trade_offs": list(candidate.trade_offs),
                        "unmet_conditions": list(candidate.unmet),
                    })
                response = {
                    "request_id": request.request_id,
                    "state": "generated",
                    "frozen_at": frozen_at,
                    "job_ids": list(request.job_ids),
                    "snapshot": snapshot,
                    "combinations": combinations,
                }
                self.connection.execute(
                    "INSERT INTO mo_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('orchestration-generate',?,?,?,?)",
                    (request.idempotency_key, request_digest, canonical_json(response), frozen_at),
                )
                self._audit(
                    "orchestration", request.request_id, "orchestration.generated", actor_id,
                    {"job_ids": list(request.job_ids), "candidates": len(combinations)},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("编排请求编号或幂等键冲突") from exc
        return response

    def orchestration(self, actor_id: str, request_id: str) -> dict[str, Any]:
        self._require(actor_id, "orchestration.read")
        row = self.connection.execute(
            "SELECT * FROM orchestration_requests WHERE request_id=?", (request_id,)
        ).fetchone()
        if row is None:
            raise NotFound("编排请求不存在")
        combinations = [
            {
                "combination_id": item["combination_id"],
                "rank": item["rank"],
                "score": item["score"],
                "assignments": json.loads(item["assignments_json"]),
                "trade_offs": json.loads(item["trade_offs_json"]),
                "unmet_conditions": json.loads(item["unmet_json"]),
            }
            for item in self.connection.execute(
                "SELECT * FROM orchestration_combinations WHERE request_id=? ORDER BY rank",
                (request_id,),
            ).fetchall()
        ]
        return {
            "request_id": row["request_id"],
            "state": row["state"],
            "revision": row["revision"],
            "frozen_at": row["frozen_at"],
            "job_ids": json.loads(row["job_ids_json"]),
            "snapshot": json.loads(row["snapshot_json"]),
            "confirmed_combination_id": row["confirmed_combination_id"],
            "combinations": combinations,
        }

    def confirm_orchestration(
        self,
        actor_id: str,
        request_id: str,
        combination_id: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require(actor_id, "orchestration.confirm")
        request_digest = digest({"request_id": request_id, "combination_id": combination_id})
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM mo_idempotency "
            "WHERE scope='orchestration-confirm' AND idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同确认内容")
            return json.loads(stored["response_json"])
        request = self.connection.execute(
            "SELECT * FROM orchestration_requests WHERE request_id=?", (request_id,)
        ).fetchone()
        if request is None:
            raise NotFound("编排请求不存在")
        if request["state"] != "generated":
            raise InvalidState(f"编排请求当前状态为 {request['state']}，不能确认")
        combination = self.connection.execute(
            "SELECT * FROM orchestration_combinations WHERE combination_id=? AND request_id=?",
            (combination_id, request_id),
        ).fetchone()
        if combination is None:
            raise NotFound("候选组合不存在")
        unmet = json.loads(combination["unmet_json"])
        if unmet:
            summary = "；".join(f"{item['job_id']}/{item['kind']}" for item in unmet)
            raise Conflict(f"候选组合存在未满足条件，不能确认：{summary}")
        assignments = json.loads(combination["assignments_json"])
        ordered = sorted(assignments, key=lambda item: (item["batch_id"], item["job_id"], item["kind"]))
        job_ids = json.loads(request["job_ids_json"])
        now = self._now()
        reservations: list[dict[str, Any]] = []
        with transaction(self.connection, immediate=True):
            expected_revisions: dict[str, int] = {}
            for assignment in ordered:
                batch_id = assignment["batch_id"]
                batch = self.connection.execute(
                    "SELECT revision,available,state FROM resource_batches WHERE batch_id=?",
                    (batch_id,),
                ).fetchone()
                if batch is None:
                    raise Conflict(f"资源批次 {batch_id} 不存在，整单确认失败")
                if batch["state"] != "active":
                    raise Conflict(f"资源批次 {batch_id} 已不可用，整单确认失败")
                expected = expected_revisions.get(batch_id, int(assignment["batch_revision"]))
                if int(batch["revision"]) != expected:
                    raise Conflict(f"资源批次 {batch_id} 版本已变化，整单确认失败")
                quantity = int(assignment["quantity"])
                if int(batch["available"]) < quantity:
                    raise Conflict(f"资源批次 {batch_id} 可用量不足，整单确认失败")
                cursor = self.connection.execute(
                    "UPDATE resource_batches SET available=available-?,revision=revision+1 "
                    "WHERE batch_id=? AND revision=?",
                    (quantity, batch_id, expected),
                )
                if cursor.rowcount != 1:
                    raise Conflict(f"资源批次 {batch_id} 版本已变化，整单确认失败")
                expected_revisions[batch_id] = expected + 1
                cursor = self.connection.execute(
                    "INSERT INTO resource_reservations(request_id,combination_id,job_id,kind,batch_id,"
                    "batch_revision,quantity,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        request_id,
                        combination_id,
                        assignment["job_id"],
                        assignment["kind"],
                        batch_id,
                        assignment["batch_revision"],
                        quantity,
                        now,
                    ),
                )
                reservations.append({
                    "reservation_id": int(cursor.lastrowid),
                    "job_id": assignment["job_id"],
                    "kind": assignment["kind"],
                    "batch_id": batch_id,
                    "batch_revision": assignment["batch_revision"],
                    "quantity": quantity,
                    "state": "reserved",
                })
            for job_id in job_ids:
                cursor = self.connection.execute(
                    "UPDATE maintenance_jobs SET state='confirmed',revision=revision+1 "
                    "WHERE job_id=? AND state='planned'",
                    (job_id,),
                )
                if cursor.rowcount != 1:
                    raise InvalidState(f"作业 {job_id} 状态已变化，整单确认失败")
            cursor = self.connection.execute(
                "UPDATE orchestration_requests SET state='confirmed',confirmed_combination_id=?,"
                "revision=revision+1 WHERE request_id=? AND state='generated'",
                (combination_id, request_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("编排请求状态已变化，整单确认失败")
            response = {
                "request_id": request_id,
                "combination_id": combination_id,
                "state": "confirmed",
                "reservations": reservations,
            }
            self.connection.execute(
                "INSERT INTO mo_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                "VALUES('orchestration-confirm',?,?,?,?)",
                (idempotency_key, request_digest, canonical_json(response), now),
            )
            self._audit(
                "orchestration", request_id, "orchestration.confirmed", actor_id,
                {"combination_id": combination_id, "reservations": len(reservations)},
            )
        return response

    def cancel_orchestration(self, actor_id: str, request_id: str, reason: str) -> dict[str, Any]:
        self._require(actor_id, "orchestration.cancel")
        if not isinstance(reason, str) or not reason.strip():
            raise ValidationFailed("取消原因不能为空")
        request = self.connection.execute(
            "SELECT * FROM orchestration_requests WHERE request_id=?", (request_id,)
        ).fetchone()
        if request is None:
            raise NotFound("编排请求不存在")
        if request["state"] == "cancelled":
            raise InvalidState("编排请求已取消")
        now = self._now()
        released: list[int] = []
        retained: list[int] = []
        with transaction(self.connection, immediate=True):
            if request["state"] == "confirmed":
                rows = self.connection.execute(
                    "SELECT * FROM resource_reservations WHERE request_id=? ORDER BY reservation_id",
                    (request_id,),
                ).fetchall()
                for reservation in rows:
                    if reservation["state"] != "reserved":
                        retained.append(int(reservation["reservation_id"]))
                        continue
                    cursor = self.connection.execute(
                        "UPDATE resource_batches SET available=available+?,revision=revision+1 "
                        "WHERE batch_id=?",
                        (reservation["quantity"], reservation["batch_id"]),
                    )
                    if cursor.rowcount != 1:
                        raise Conflict(f"资源批次 {reservation['batch_id']} 释放失败")
                    self.connection.execute(
                        "UPDATE resource_reservations SET state='released',released_at=? "
                        "WHERE reservation_id=? AND state='reserved'",
                        (now, reservation["reservation_id"]),
                    )
                    released.append(int(reservation["reservation_id"]))
                for job_id in json.loads(request["job_ids_json"]):
                    self.connection.execute(
                        "UPDATE maintenance_jobs SET state='cancelled',revision=revision+1 "
                        "WHERE job_id=? AND state='confirmed'",
                        (job_id,),
                    )
            cursor = self.connection.execute(
                "UPDATE orchestration_requests SET state='cancelled',revision=revision+1 "
                "WHERE request_id=? AND state=?",
                (request_id, request["state"]),
            )
            if cursor.rowcount != 1:
                raise InvalidState("编排请求状态已变化，取消失败")
            self._audit(
                "orchestration", request_id, "orchestration.cancelled", actor_id,
                {"reason": reason.strip(), "released": released, "retained_consumed": retained},
            )
        return {
            "request_id": request_id,
            "state": "cancelled",
            "released_reservations": released,
            "retained_consumed_reservations": retained,
        }

    def start_job(self, actor_id: str, job_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "job.start")
        self._job_row(job_id)
        now = self._now()
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE maintenance_jobs SET state='started',revision=revision+1 "
                "WHERE job_id=? AND state='confirmed' AND revision=?",
                (job_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("作业不是当前已确认版本，不能开工")
            cursor = self.connection.execute(
                "UPDATE resource_reservations SET state='consumed',consumed_at=? "
                "WHERE job_id=? AND state='reserved'",
                (now, job_id),
            )
            consumed = cursor.rowcount
            self._audit("job", job_id, "job.started", actor_id, {"consumed_reservations": consumed})
        return {"job_id": job_id, "state": "started", "revision": expected_revision + 1,
                "consumed_reservations": consumed}

    def complete_job(self, actor_id: str, job_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "job.complete")
        self._job_row(job_id)
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE maintenance_jobs SET state='completed',revision=revision+1 "
                "WHERE job_id=? AND state='started' AND revision=?",
                (job_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("作业不是当前已开工版本，不能完工")
            self._audit("job", job_id, "job.completed", actor_id, {})
        return {"job_id": job_id, "state": "completed", "revision": expected_revision + 1}

    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute("SELECT * FROM mo_audit_events ORDER BY event_id").fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        return {"valid": valid, "events": len(rows), "head_hash": previous_hash}
