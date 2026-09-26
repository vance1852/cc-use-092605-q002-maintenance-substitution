"""检修资源替代编排的事务用例。

业务主线：
1. 计划人员登记资源批次与检修作业（依赖、海况窗口、最低资质、备件兼容范围、降级顺序）；
2. 基于作业冻结内容生成计划版本；
3. 按资源当前版本快照生成候选组合，逐项说明批次来源与未满足条件；
4. 确认时在单事务内乐观锁定全部关联资源，任一资源版本变化则整单失败；
5. 开工消耗预留，已开工作业在后续候选中固定不迁移；取消只释放未消耗预留；
6. 相同业务请求（幂等键）重放返回首次结果，不重复占用。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .clock import SystemClock, utc_text
from .contracts import MaintenanceOperation, OperationRequirement, ResourceCandidate, TimeWindow
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .orchestration import (
    ResourceSnapshot,
    canonical_json,
    digest,
    decimal_text,
    plan_candidates,
    windows_overlap,
)
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"resource.write", "operation.write", "plan.write", "plan.run"},
    "dispatcher": {"order.confirm", "order.cancel", "operation.start"},
    "risk": {"report.read"},
    "auditor": {"report.read", "audit.read"},
}


class MaintenanceOrchestrationService:
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

    # ------------------------------------------------------------------ 用户

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

    # -------------------------------------------------------------- 资源批次

    def register_resource(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "resource.write")
        resource = ResourceCandidate.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO resource_batches(resource_id,kind,batch_id,capability_mw,sea_state_max_m,"
                    "available_from,available_to,certifications_json,compat_models_json,note,revision,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,1,?,?)",
                    (
                        resource.resource_id,
                        resource.kind,
                        resource.batch_id,
                        None if resource.capability_mw is None else decimal_text(resource.capability_mw),
                        None if resource.sea_state_max_m is None else decimal_text(resource.sea_state_max_m),
                        resource.available_from,
                        resource.available_to,
                        canonical_json(sorted(resource.certifications)),
                        canonical_json(sorted(resource.compat_models)),
                        resource.note,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("resource", resource.resource_id, "resource.registered", actor_id, {
                    "kind": resource.kind, "batch_id": resource.batch_id,
                })
        except sqlite3.IntegrityError as exc:
            raise Conflict("资源编号已经存在") from exc
        return self.resource(resource.resource_id)

    def update_resource(self, actor_id: str, resource_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """登记批次的临时变化（容量、浪高上限、停用），版本号递增使旧候选确认失败。"""
        self._require(actor_id, "resource.write")
        existing = self._resource_row(resource_id)
        fields: list[str] = []
        values: list[Any] = []
        if "capacity" in raw:
            capacity = raw["capacity"]
            if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
                raise ValidationFailed("capacity 必须是正整数")
            fields.append("capacity=?")
            values.append(capacity)
        if "sea_state_max_m" in raw:
            sea_state = raw["sea_state_max_m"]
            try:
                sea_state_decimal = Decimal(str(sea_state))
            except (ArithmeticError, ValueError, TypeError) as exc:
                raise ValidationFailed("sea_state_max_m 必须是十进制数值") from exc
            if sea_state_decimal < 0:
                raise ValidationFailed("sea_state_max_m 不能为负数")
            fields.append("sea_state_max_m=?")
            values.append(decimal_text(sea_state_decimal))
        if "active" in raw:
            active = raw["active"]
            if not isinstance(active, bool):
                raise ValidationFailed("active 必须是布尔值")
            fields.append("active=?")
            values.append(1 if active else 0)
        if "note" in raw:
            note = raw["note"]
            if not isinstance(note, str):
                raise ValidationFailed("note 必须是字符串")
            fields.append("note=?")
            values.append(note.strip())
        if not fields:
            raise ValidationFailed("没有可更新的批次字段")
        fields.append("revision=revision+1")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                f"UPDATE resource_batches SET {', '.join(fields)} WHERE resource_id=? AND revision=?",
                (*values, resource_id, existing["revision"]),
            )
            if cursor.rowcount != 1:
                raise Conflict("资源版本已变化，请重试")
            self._audit("resource", resource_id, "resource.revised", actor_id, {
                "from_revision": existing["revision"], "to_revision": existing["revision"] + 1,
                "changes": sorted(key for key in raw if key in {"capacity", "sea_state_max_m", "active", "note"}),
            })
        return self.resource(resource_id)

    def _resource_row(self, resource_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM resource_batches WHERE resource_id=?", (resource_id,)
        ).fetchone()
        if row is None:
            raise NotFound("资源不存在")
        return row

    def resource(self, resource_id: str) -> dict[str, Any]:
        row = self._resource_row(resource_id)
        return self._resource_view(row)

    @staticmethod
    def _resource_view(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "resource_id": row["resource_id"],
            "kind": row["kind"],
            "batch_id": row["batch_id"],
            "capability_mw": row["capability_mw"],
            "sea_state_max_m": row["sea_state_max_m"],
            "available_from": row["available_from"],
            "available_to": row["available_to"],
            "certifications": json.loads(row["certifications_json"]),
            "compat_models": json.loads(row["compat_models_json"]),
            "capacity": row["capacity"],
            "active": bool(row["active"]),
            "revision": row["revision"],
        }

    # -------------------------------------------------------------- 检修作业

    def register_operation(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "operation.write")
        operation = MaintenanceOperation.from_dict(raw)
        for dependency in operation.depends_on:
            if self.connection.execute(
                "SELECT 1 FROM maintenance_operations WHERE operation_id=?", (dependency,)
            ).fetchone() is None:
                raise ValidationFailed(f"依赖作业 {dependency} 尚未登记")
        for requirement in operation.requirements:
            for resource_id in requirement.preference_order:
                row = self.connection.execute(
                    "SELECT kind,active FROM resource_batches WHERE resource_id=?", (resource_id,)
                ).fetchone()
                if row is None:
                    raise ValidationFailed(f"降级顺序中的资源 {resource_id} 尚未登记")
                if row["kind"] != requirement.kind:
                    raise ValidationFailed(f"资源 {resource_id} 类型与环节 {requirement.requirement_id} 不匹配")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO maintenance_operations(operation_id,turbine_model,sea_window_starts_at,"
                    "sea_window_ends_at,depends_on_json,state,revision,created_by,created_at) "
                    "VALUES(?,?,?,?,?,'registered',1,?,?)",
                    (
                        operation.operation_id,
                        operation.turbine_model,
                        operation.sea_window.starts_at,
                        operation.sea_window.ends_at,
                        canonical_json(sorted(operation.depends_on)),
                        actor_id,
                        self._now(),
                    ),
                )
                for ordinal, requirement in enumerate(operation.requirements):
                    self.connection.execute(
                        "INSERT INTO operation_requirements(requirement_id,operation_id,kind,"
                        "minimum_certification,minimum_capability_mw,required_model,sea_state_limit_m,"
                        "preference_order_json,essential,ordinal) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            requirement.requirement_id,
                            operation.operation_id,
                            requirement.kind,
                            requirement.minimum_certification,
                            None if requirement.minimum_capability_mw is None else decimal_text(requirement.minimum_capability_mw),
                            requirement.required_model,
                            None if requirement.sea_state_limit_m is None else decimal_text(requirement.sea_state_limit_m),
                            canonical_json(list(requirement.preference_order)),
                            1 if requirement.essential else 0,
                            ordinal,
                        ),
                    )
                self._audit("operation", operation.operation_id, "operation.registered", actor_id, {
                    "requirements": len(operation.requirements),
                    "depends_on": sorted(operation.depends_on),
                })
        except sqlite3.IntegrityError as exc:
            raise Conflict("作业编号或环节编号冲突") from exc
        return self.operation(operation.operation_id)

    def _operation_row(self, operation_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM maintenance_operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if row is None:
            raise NotFound("检修作业不存在")
        return row

    def operation(self, operation_id: str) -> dict[str, Any]:
        row = self._operation_row(operation_id)
        requirement_rows = self.connection.execute(
            "SELECT * FROM operation_requirements WHERE operation_id=? ORDER BY ordinal",
            (operation_id,),
        ).fetchall()
        return {
            "operation_id": row["operation_id"],
            "turbine_model": row["turbine_model"],
            "sea_window": {"starts_at": row["sea_window_starts_at"], "ends_at": row["sea_window_ends_at"]},
            "depends_on": json.loads(row["depends_on_json"]),
            "state": row["state"],
            "revision": row["revision"],
            "started_at": row["started_at"],
            "requirements": [
                {
                    "requirement_id": item["requirement_id"],
                    "kind": item["kind"],
                    "minimum_certification": item["minimum_certification"],
                    "minimum_capability_mw": item["minimum_capability_mw"],
                    "required_model": item["required_model"],
                    "sea_state_limit_m": item["sea_state_limit_m"],
                    "preference_order": json.loads(item["preference_order_json"]),
                    "essential": bool(item["essential"]),
                }
                for item in requirement_rows
            ],
        }

    def start_operation(self, actor_id: str, operation_id: str) -> dict[str, Any]:
        """开工：作业预留由 reserved 转为 consumed，已消耗预留不再允许迁移或释放。"""
        self._require(actor_id, "operation.start")
        with transaction(self.connection, immediate=True):
            row = self._operation_row(operation_id)
            if row["state"] == "started":
                raise InvalidState("作业已经开工")
            if row["state"] != "registered":
                raise InvalidState(f"作业状态为 {row['state']}，不能开工")
            reservations = self.connection.execute(
                "SELECT * FROM resource_reservations WHERE operation_id=? AND state='reserved'",
                (operation_id,),
            ).fetchall()
            if not reservations:
                raise InvalidState("作业尚未确认资源占用，不能开工")
            now = self._now()
            self.connection.execute(
                "UPDATE resource_reservations SET state='consumed',consumed_at=?,revision=revision+1 "
                "WHERE operation_id=? AND state='reserved'",
                (now, operation_id),
            )
            self.connection.execute(
                "UPDATE maintenance_operations SET state='started',started_at=?,revision=revision+1 "
                "WHERE operation_id=?",
                (now, operation_id),
            )
            self._audit("operation", operation_id, "operation.started", actor_id, {
                "consumed_reservations": len(reservations),
            })
        return self.operation(operation_id)

    # -------------------------------------------------------------- 冻结计划

    def freeze_plan(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "plan.write")
        plan_id = raw.get("plan_id")
        if not isinstance(plan_id, str) or not plan_id.strip():
            raise ValidationFailed("plan_id 不能为空")
        plan_id = plan_id.strip()
        operation_ids = raw.get("operation_ids")
        if not isinstance(operation_ids, list) or not operation_ids:
            raise ValidationFailed("operation_ids 必须是非空数组")
        ordered_ids: list[str] = []
        for item in operation_ids:
            if not isinstance(item, str) or not item.strip():
                raise ValidationFailed("operation_ids 元素必须是编号")
            operation_id = item.strip()
            if operation_id in ordered_ids:
                raise ValidationFailed(f"作业 {operation_id} 在计划中重复")
            ordered_ids.append(operation_id)
        rows = {operation_id: self._operation_row(operation_id) for operation_id in ordered_ids}
        for operation_id, row in rows.items():
            if row["state"] != "registered":
                raise InvalidState(f"作业 {operation_id} 状态为 {row['state']}，不能纳入新冻结计划")
        self._assert_dependency_order(ordered_ids, rows)
        definition = [self._plan_operation_definition(operation_id) for operation_id in ordered_ids]
        content_sha256 = digest(definition)
        existing = self.connection.execute(
            "SELECT plan_id FROM maintenance_plans WHERE content_sha256=?", (content_sha256,)
        ).fetchone()
        if existing is not None:
            raise Conflict("相同作业内容已冻结为计划 " + existing["plan_id"])
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO maintenance_plans(plan_id,operation_ids_json,definition_json,content_sha256,"
                    "state,frozen_version,created_by,created_at) VALUES(?,?,?,?,'frozen',1,?,?)",
                    (
                        plan_id,
                        canonical_json(ordered_ids),
                        canonical_json(definition),
                        content_sha256,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("plan", plan_id, "plan.frozen", actor_id, {
                    "operations": ordered_ids, "sha256": content_sha256,
                })
        except sqlite3.IntegrityError as exc:
            raise Conflict("计划编号冲突") from exc
        return self.plan(plan_id)

    def _assert_dependency_order(
        self,
        ordered_ids: Sequence[str],
        rows: Mapping[str, sqlite3.Row],
    ) -> None:
        positions = {operation_id: index for index, operation_id in enumerate(ordered_ids)}
        for operation_id in ordered_ids:
            for dependency in json.loads(rows[operation_id]["depends_on_json"]):
                if dependency in positions and positions[dependency] >= positions[operation_id]:
                    raise ValidationFailed(f"作业 {operation_id} 必须排在依赖作业 {dependency} 之后")
        # 环检测（计划内依赖图）

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(node: str) -> None:
            if node in visited:
                return
            if node in visiting:
                raise ValidationFailed("作业依赖存在环")
            visiting.add(node)
            for dependency in json.loads(rows[node]["depends_on_json"]):
                if dependency in positions:
                    visit(dependency)
            visiting.discard(node)
            visited.add(node)

        for operation_id in ordered_ids:
            visit(operation_id)

    def _plan_operation_definition(self, operation_id: str) -> dict[str, Any]:
        row = self._operation_row(operation_id)
        requirement_rows = self.connection.execute(
            "SELECT * FROM operation_requirements WHERE operation_id=? ORDER BY ordinal",
            (operation_id,),
        ).fetchall()
        return {
            "operation_id": operation_id,
            "turbine_model": row["turbine_model"],
            "sea_window": {"starts_at": row["sea_window_starts_at"], "ends_at": row["sea_window_ends_at"]},
            "depends_on": json.loads(row["depends_on_json"]),
            "requirements": [
                {
                    "requirement_id": item["requirement_id"],
                    "kind": item["kind"],
                    "minimum_certification": item["minimum_certification"],
                    "minimum_capability_mw": item["minimum_capability_mw"],
                    "required_model": item["required_model"],
                    "sea_state_limit_m": item["sea_state_limit_m"],
                    "preference_order": json.loads(item["preference_order_json"]),
                    "essential": bool(item["essential"]),
                }
                for item in requirement_rows
            ],
        }

    def _plan_row(self, plan_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM maintenance_plans WHERE plan_id=?", (plan_id,)
        ).fetchone()
        if row is None:
            raise NotFound("检修计划不存在")
        return row

    def plan(self, plan_id: str) -> dict[str, Any]:
        row = self._plan_row(plan_id)
        return {
            "plan_id": row["plan_id"],
            "operation_ids": json.loads(row["operation_ids_json"]),
            "state": row["state"],
            "frozen_version": row["frozen_version"],
            "content_sha256": row["content_sha256"],
            "created_at": row["created_at"],
        }

    # ------------------------------------------------------------ 候选组合

    def _load_operations(self, operation_ids: Sequence[str]) -> list[MaintenanceOperation]:
        operations: list[MaintenanceOperation] = []
        for operation_id in operation_ids:
            row = self._operation_row(operation_id)
            requirement_rows = self.connection.execute(
                "SELECT * FROM operation_requirements WHERE operation_id=? ORDER BY ordinal",
                (operation_id,),
            ).fetchall()
            requirements = tuple(
                OperationRequirement(
                    requirement_id=item["requirement_id"],
                    kind=item["kind"],
                    minimum_certification=item["minimum_certification"],
                    minimum_capability_mw=None if item["minimum_capability_mw"] is None else Decimal(item["minimum_capability_mw"]),
                    required_model=item["required_model"],
                    sea_state_limit_m=None if item["sea_state_limit_m"] is None else Decimal(item["sea_state_limit_m"]),
                    preference_order=tuple(json.loads(item["preference_order_json"])),
                    essential=bool(item["essential"]),
                )
                for item in requirement_rows
            )
            operations.append(MaintenanceOperation(
                operation_id=row["operation_id"],
                turbine_model=row["turbine_model"],
                sea_window=TimeWindow(row["sea_window_starts_at"], row["sea_window_ends_at"]),
                requirements=requirements,
                depends_on=frozenset(json.loads(row["depends_on_json"])),
            ))
        return operations

    def _external_usages(self, operation_ids: Sequence[str]) -> dict[str, list[tuple[str, str]]]:
        """计划外订单的有效占用（已预留或已消耗），按资源聚合为海况窗口。

        计划内作业的占用不在这里：已开工作业的消耗占用通过 pinned 传入引擎，
        未开工作业的占用属于候选正在重排的对象，二者都不能当作外部容量。
        """
        placeholders = ",".join("?" for _ in operation_ids)
        sql = (
            "SELECT rr.resource_id, o.sea_window_starts_at, o.sea_window_ends_at "
            "FROM resource_reservations rr "
            "JOIN maintenance_operations o ON o.operation_id=rr.operation_id "
            f"WHERE rr.state IN ('reserved','consumed') AND rr.operation_id NOT IN ({placeholders})"
        )
        rows = self.connection.execute(sql, tuple(operation_ids)).fetchall()
        usages: dict[str, list[tuple[str, str]]] = {}
        for row in rows:
            usages.setdefault(row["resource_id"], []).append(
                (row["sea_window_starts_at"], row["sea_window_ends_at"])
            )
        return usages

    def generate_candidates(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        self._require(actor_id, "plan.run")
        plan = self._plan_row(plan_id)
        if plan["state"] != "frozen":
            raise InvalidState("只有冻结状态的计划可以生成候选")
        operation_ids = json.loads(plan["operation_ids_json"])
        operations = self._load_operations(operation_ids)

        referenced: set[str] = set()
        for operation in operations:
            for requirement in operation.requirements:
                referenced.update(requirement.preference_order)
        resources = {
            row["resource_id"]: ResourceSnapshot.from_row(row)
            for row in self.connection.execute(
                "SELECT * FROM resource_batches WHERE resource_id IN ({})".format(
                    ",".join("?" for _ in referenced)
                ),
                tuple(sorted(referenced)),
            ).fetchall()
        } if referenced else {}

        operation_rows = {
            operation.operation_id: self._operation_row(operation.operation_id)
            for operation in operations
        }
        # 已有活动预留（reserved/consumed）的作业固定不迁移：未开工者须先取消订单
        # 释放预留后才能重排；已开工者取消也不会释放，因此永远固定。
        pinned: dict[str, dict[str, tuple[str, str | None, int]]] = {}
        pinned_detail: list[dict[str, Any]] = []
        for operation in operations:
            reservation_rows = self.connection.execute(
                "SELECT * FROM resource_reservations WHERE operation_id=? "
                "AND state IN ('reserved','consumed') ORDER BY ordinal",
                (operation.operation_id,),
            ).fetchall()
            if not reservation_rows:
                continue
            choice: dict[str, tuple[str, str | None, int]] = {}
            reservations_detail: list[dict[str, Any]] = []
            requirements = {item.requirement_id: item for item in operation.requirements}
            states = {row["state"] for row in reservation_rows}
            for item in reservation_rows:
                requirement = requirements.get(item["requirement_id"])
                rank = (
                    requirement.preference_order.index(item["resource_id"])
                    if requirement is not None and item["resource_id"] in requirement.preference_order
                    else (len(requirement.preference_order) if requirement is not None else 0)
                )
                choice[item["requirement_id"]] = (item["resource_id"], item["batch_id"], rank)
                reservations_detail.append({
                    "requirement_id": item["requirement_id"],
                    "kind": item["kind"],
                    "resource_id": item["resource_id"],
                    "batch_id": item["batch_id"],
                    "resource_revision": item["resource_revision"],
                    "reservation_state": item["state"],
                    "order_id": item["order_id"],
                })
            pinned[operation.operation_id] = choice
            if "consumed" in states:
                reason = "作业已经开工，资源占用固定，不参与自动迁移"
            else:
                reason = "作业已确认工单占用资源；如需替代组合须先取消工单释放未消耗预留"
            pinned_detail.append({
                "operation_id": operation.operation_id,
                "operation_state": operation_rows[operation.operation_id]["state"],
                "reason": reason,
                "reservations": reservations_detail,
            })

        external_usages = self._external_usages(operation_ids)
        engine_result = plan_candidates(operations, resources, external_usages, pinned)

        operation_states = {
            operation.operation_id: {
                "state": self._operation_row(operation.operation_id)["state"],
                "revision": self._operation_row(operation.operation_id)["revision"],
            }
            for operation in operations
        }
        resource_versions = {
            resource_id: snapshot.revision for resource_id, snapshot in resources.items()
        }
        result = {
            "plan_id": plan_id,
            "frozen_version": plan["frozen_version"],
            "generated_at": self._now(),
            "feasible": engine_result["feasible"],
            "pinned_operations": pinned_detail,
            "operations": engine_result["operations"],
            "candidates": engine_result["candidates"],
        }
        input_value = {
            "plan_sha256": plan["content_sha256"],
            "operation_states": operation_states,
            "resources": [
                self._resource_view(self._resource_row(resource_id))
                for resource_id in sorted(referenced)
            ],
            "external_usages": {
                key: sorted(value) for key, value in sorted(external_usages.items())
            },
            "pinned": pinned_detail,
        }
        input_sha256 = digest(input_value)
        with transaction(self.connection, immediate=True):
            existing = self.connection.execute(
                "SELECT run_id,result_json FROM plan_candidate_runs WHERE plan_id=? AND input_sha256=?",
                (plan_id, input_sha256),
            ).fetchone()
            if existing is not None:
                return {"run_id": existing["run_id"], **json.loads(existing["result_json"]), "replayed": True}
            cursor = self.connection.execute(
                "INSERT INTO plan_candidate_runs(plan_id,input_sha256,resource_versions_json,feasible,"
                "result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (
                    plan_id,
                    input_sha256,
                    canonical_json(resource_versions),
                    1 if result["feasible"] else 0,
                    canonical_json(result),
                    actor_id,
                    self._now(),
                ),
            )
            run_id = int(cursor.lastrowid)
            self._audit("plan", plan_id, "candidates.generated", actor_id, {
                "run_id": run_id, "feasible": result["feasible"],
                "candidate_count": len(result["candidates"]),
            })
        return {"run_id": run_id, **result, "replayed": False}

    def candidate_run(self, run_id: int) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM plan_candidate_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise NotFound("候选运行不存在")
        return {"run_id": row["run_id"], "plan_id": row["plan_id"], **json.loads(row["result_json"])}

    # ------------------------------------------------------------ 确认占用

    def confirm_order(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "order.confirm")
        order_id = raw.get("order_id")
        run_id = raw.get("candidate_run_id")
        idempotency_key = raw.get("idempotency_key")
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValidationFailed("order_id 不能为空")
        if isinstance(run_id, bool) or not isinstance(run_id, int):
            raise ValidationFailed("candidate_run_id 必须是整数")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValidationFailed("idempotency_key 不能为空")
        order_id = order_id.strip()
        idempotency_key = idempotency_key.strip()
        ordinal = raw.get("candidate_ordinal", 1)
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal <= 0:
            raise ValidationFailed("candidate_ordinal 必须是正整数")
        request_digest = digest({
            "order_id": order_id,
            "candidate_run_id": run_id,
            "candidate_ordinal": ordinal,
        })

        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM maintenance_orders WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同的确认请求")
            return json.loads(stored["response_json"])

        run = self.connection.execute(
            "SELECT * FROM plan_candidate_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if run is None:
            raise NotFound("候选运行不存在")
        if run["plan_id"] != raw.get("plan_id"):
            raise ValidationFailed("候选运行不属于该计划")
        result = json.loads(run["result_json"])
        candidate = next(
            (item for item in result["candidates"] if item["ordinal"] == ordinal), None
        )
        if candidate is None:
            raise NotFound("候选组合序号不存在")
        if not candidate["feasible"]:
            raise InvalidState("候选组合存在资源冲突，不能确认")

        response: dict[str, Any] = {}
        try:
            with transaction(self.connection, immediate=True):
                plan = self._plan_row(run["plan_id"])
                if plan["state"] != "frozen":
                    raise InvalidState("计划不是冻结状态")
                operation_ids = json.loads(plan["operation_ids_json"])
                pinned_ids = {item["operation_id"] for item in result["pinned_operations"]}
                selections = candidate["selections"]

                # 1) 固定作业（已有活动预留）跳过不重复占用；其余作业必须仍是 registered。
                current_windows: dict[str, tuple[str, str]] = {}
                target_operations: list[str] = []
                for operation_id in operation_ids:
                    operation_row = self._operation_row(operation_id)
                    current_windows[operation_id] = (
                        operation_row["sea_window_starts_at"],
                        operation_row["sea_window_ends_at"],
                    )
                    if operation_id in pinned_ids:
                        continue
                    if operation_row["state"] != "registered":
                        raise InvalidState(
                            f"作业 {operation_id} 状态为 {operation_row['state']}，候选版本过期，整单确认失败"
                        )
                    target_operations.append(operation_id)

                # 2) 收集本单全部资源行，校验候选快照中的资源版本未发生任何变化。
                chosen_resources: dict[str, dict[str, Any]] = {}
                occupied_items: list[dict[str, Any]] = []
                for operation_id in target_operations:
                    operation_choice = selections.get(operation_id)
                    if not operation_choice:
                        raise InvalidState(f"候选组合缺少作业 {operation_id} 的资源选择")
                    for requirement_id, chosen in operation_choice.items():
                        resource_id = chosen["resource_id"]
                        resource_row = self._resource_row(resource_id)
                        snapshot_revision = json.loads(run["resource_versions_json"]).get(resource_id)
                        if snapshot_revision is None or resource_row["revision"] != snapshot_revision:
                            raise Conflict(
                                f"资源 {resource_id} 版本已变化（候选基于修订 {snapshot_revision}，"
                                f"当前修订 {resource_row['revision']}），整单确认失败"
                            )
                        if not resource_row["active"]:
                            raise Conflict(f"资源 {resource_id} 批次已停用，整单确认失败")
                        chosen_resources.setdefault(resource_id, {
                            "row": resource_row,
                            "windows": [],
                        })["windows"].append(current_windows[operation_id])
                        occupied_items.append({
                            "operation_id": operation_id,
                            "requirement_id": requirement_id,
                            "kind": chosen["kind"],
                            "essential": chosen["essential"],
                            "resource_id": resource_id,
                            "batch_id": resource_row["batch_id"],
                            "resource_revision": resource_row["revision"],
                            "preference_rank": chosen["preference_rank"],
                        })

                # 3) 同一作业环节不能存在另一张未取消订单的有效预留。
                for item in occupied_items:
                    clash = self.connection.execute(
                        "SELECT 1 FROM resource_reservations WHERE operation_id=? AND requirement_id=? "
                        "AND state IN ('reserved','consumed') LIMIT 1",
                        (item["operation_id"], item["requirement_id"]),
                    ).fetchone()
                    if clash is not None:
                        raise Conflict(
                            f"作业 {item['operation_id']} 环节 {item['requirement_id']} 已被未取消订单占用"
                        )

                # 4) 基于已提交预留复核批次容量（海况窗口重叠计数）。
                for resource_id, bundle in chosen_resources.items():
                    capacity = int(bundle["row"]["capacity"])
                    existing = self.connection.execute(
                        "SELECT o.sea_window_starts_at, o.sea_window_ends_at "
                        "FROM resource_reservations rr "
                        "JOIN maintenance_operations o ON o.operation_id=rr.operation_id "
                        "WHERE rr.resource_id=? AND rr.state IN ('reserved','consumed')",
                        (resource_id,),
                    ).fetchall()
                    existing_windows = [
                        (row["sea_window_starts_at"], row["sea_window_ends_at"]) for row in existing
                    ]
                    new_windows = bundle["windows"]
                    for index, window in enumerate(new_windows):
                        overlapping = sum(
                            1 for other_index, other in enumerate(new_windows)
                            if other_index != index and windows_overlap(window, other)
                        ) + sum(1 for other in existing_windows if windows_overlap(window, other))
                        if overlapping + 1 > capacity:
                            raise Conflict(
                                f"资源 {resource_id} 在海况窗口内容量不足（批次容量 {capacity}），整单确认失败"
                            )

                if not occupied_items:
                    raise InvalidState("候选组合中的作业均已占用，没有需要确认的资源预留")

                # 5) 全部通过：原子写入订单与预留。
                now = self._now()
                cursor = self.connection.execute(
                    "INSERT INTO maintenance_orders(order_id,plan_id,candidate_run_id,candidate_ordinal,"
                    "idempotency_key,request_sha256,response_json,state,revision,confirmed_by,confirmed_at) "
                    "VALUES(?,?,?,?,?,?,?,'confirmed',1,?,?)",
                    (
                        order_id,
                        run["plan_id"],
                        run_id,
                        ordinal,
                        idempotency_key,
                        request_digest,
                        canonical_json({"pending": True}),
                        actor_id,
                        now,
                    ),
                )
                for item_ordinal, item in enumerate(occupied_items, start=1):
                    self.connection.execute(
                        "INSERT INTO resource_reservations(order_id,operation_id,requirement_id,resource_id,"
                        "batch_id,kind,essential,quantity,state,ordinal,resource_revision,revision) "
                        "VALUES(?,?,?,?,?,?,?,1,'reserved',?,?,1)",
                        (
                            order_id,
                            item["operation_id"],
                            item["requirement_id"],
                            item["resource_id"],
                            item["batch_id"],
                            item["kind"],
                            1 if item["essential"] else 0,
                            item_ordinal,
                            item["resource_revision"],
                        ),
                    )
                response = self._order_view(self._order_row(order_id))
                self.connection.execute(
                    "UPDATE maintenance_orders SET response_json=? WHERE order_id=?",
                    (canonical_json(response), order_id),
                )
                self._audit("order", order_id, "order.confirmed", actor_id, {
                    "plan_id": run["plan_id"],
                    "candidate_run_id": run_id,
                    "candidate_ordinal": ordinal,
                    "reservations": len(occupied_items),
                    "idempotency_key": idempotency_key,
                })
        except sqlite3.IntegrityError as exc:
            raise Conflict("订单编号或幂等键冲突") from exc
        return response

    def _order_row(self, order_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM maintenance_orders WHERE order_id=?", (order_id,)
        ).fetchone()
        if row is None:
            raise NotFound("检修工单不存在")
        return row

    def _order_view(self, row: sqlite3.Row) -> dict[str, Any]:
        reservations = self.connection.execute(
            "SELECT * FROM resource_reservations WHERE order_id=? ORDER BY ordinal",
            (row["order_id"],),
        ).fetchall()
        return {
            "order_id": row["order_id"],
            "plan_id": row["plan_id"],
            "candidate_run_id": row["candidate_run_id"],
            "candidate_ordinal": row["candidate_ordinal"],
            "state": row["state"],
            "revision": row["revision"],
            "confirmed_at": row["confirmed_at"],
            "cancelled_at": row["cancelled_at"],
            "cancel_reason": row["cancel_reason"],
            "idempotency_key": row["idempotency_key"],
            "reservations": [
                {
                    "operation_id": item["operation_id"],
                    "requirement_id": item["requirement_id"],
                    "kind": item["kind"],
                    "essential": bool(item["essential"]),
                    "resource_id": item["resource_id"],
                    "batch_id": item["batch_id"],
                    "resource_revision": item["resource_revision"],
                    "state": item["state"],
                    "consumed_at": item["consumed_at"],
                    "released_at": item["released_at"],
                    "release_reason": item["release_reason"],
                }
                for item in reservations
            ],
        }

    def order(self, order_id: str) -> dict[str, Any]:
        return self._order_view(self._order_row(order_id))

    def cancel_order(self, actor_id: str, order_id: str, reason: str) -> dict[str, Any]:
        self._require(actor_id, "order.cancel")
        if not isinstance(reason, str) or not reason.strip():
            raise ValidationFailed("取消原因不能为空")
        with transaction(self.connection, immediate=True):
            row = self._order_row(order_id)
            if row["state"] == "cancelled":
                raise InvalidState("工单已经取消")
            now = self._now()
            releasable = self.connection.execute(
                "SELECT reservation_id FROM resource_reservations WHERE order_id=? AND state='reserved'",
                (order_id,),
            ).fetchall()
            self.connection.execute(
                "UPDATE resource_reservations SET state='released',released_at=?,release_reason=?,"
                "revision=revision+1 WHERE order_id=? AND state='reserved'",
                (now, reason.strip(), order_id),
            )
            self.connection.execute(
                "UPDATE maintenance_orders SET state='cancelled',cancelled_by=?,cancelled_at=?,"
                "cancel_reason=?,revision=revision+1 WHERE order_id=?",
                (actor_id, now, reason.strip(), order_id),
            )
            self._audit("order", order_id, "order.cancelled", actor_id, {
                "released_reservations": len(releasable),
                "kept_consumed_reservations": self.connection.execute(
                    "SELECT count(*) FROM resource_reservations WHERE order_id=? AND state='consumed'",
                    (order_id,),
                ).fetchone()[0],
                "reason": reason.strip(),
            })
        return self.order(order_id)

    # ---------------------------------------------------------------- 审计

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
