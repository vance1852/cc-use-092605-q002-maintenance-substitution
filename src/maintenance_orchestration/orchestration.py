"""确定性的检修资源替代组合编排。

引擎不接触数据库：服务层把冻结作业、资源批次当前快照和其他订单的占用
以普通字典传入，引擎输出可解释的候选组合。所有结论均可由输入确定性复算。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from itertools import product
from typing import Any, Iterable, Mapping, Sequence

from .contracts import MaintenanceOperation, OperationRequirement


MAX_OPERATION_COMBINATIONS = 20
MAX_PLAN_CANDIDATES = 64
MAX_RANK_OPTIONS = 8


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def windows_overlap(left: tuple[str, str], right: tuple[str, str]) -> bool:
    return left[0] < right[1] and right[0] < left[1]


@dataclass(frozen=True, slots=True)
class ResourceSnapshot:
    resource_id: str
    kind: str
    batch_id: str
    capability_mw: Decimal | None
    sea_state_max_m: Decimal | None
    available_from: str | None
    available_to: str | None
    certifications: frozenset[str]
    compat_models: frozenset[str]
    capacity: int
    revision: int
    active: bool

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ResourceSnapshot":
        import json as _json

        return cls(
            resource_id=str(row["resource_id"]),
            kind=str(row["kind"]),
            batch_id=str(row["batch_id"]),
            capability_mw=None if row["capability_mw"] is None else Decimal(str(row["capability_mw"])),
            sea_state_max_m=None if row["sea_state_max_m"] is None else Decimal(str(row["sea_state_max_m"])),
            available_from=row["available_from"],
            available_to=row["available_to"],
            certifications=frozenset(_json.loads(row["certifications_json"] or "[]")),
            compat_models=frozenset(_json.loads(row["compat_models_json"] or "[]")),
            capacity=int(row["capacity"]),
            revision=int(row["revision"]),
            active=bool(row["active"]),
        )


def _eligibility_reasons(
    requirement: OperationRequirement,
    resource: ResourceSnapshot | None,
    *,
    operation_window: tuple[str, str],
    overlapping_usage: int,
) -> list[dict[str, str]]:
    """返回未满足条件；空列表表示资源可用。"""
    reasons: list[dict[str, str]] = []
    if resource is None:
        reasons.append({"code": "not_registered", "message": "资源不存在或未登记"})
        return reasons
    if resource.kind != requirement.kind:
        reasons.append({"code": "wrong_kind", "message": f"资源类型为 {resource.kind}，环节要求 {requirement.kind}"})
    if not resource.active:
        reasons.append({"code": "inactive", "message": "资源批次已停用"})
    if requirement.minimum_capability_mw is not None:
        if resource.capability_mw is None or resource.capability_mw < requirement.minimum_capability_mw:
            reasons.append({
                "code": "capability_insufficient",
                "message": (
                    f"承载能力不足：环节要求不低于 {decimal_text(requirement.minimum_capability_mw)} 兆瓦，"
                    f"批次为 {decimal_text(resource.capability_mw)} 兆瓦"
                ),
            })
    if requirement.minimum_certification is not None:
        if requirement.minimum_certification not in resource.certifications:
            reasons.append({
                "code": "certification_missing",
                "message": f"班组缺少最低资质 {requirement.minimum_certification}",
            })
    if requirement.required_model is not None:
        if requirement.required_model not in resource.compat_models:
            reasons.append({
                "code": "model_incompatible",
                "message": f"备件兼容范围不含机型 {requirement.required_model}",
            })
    if requirement.sea_state_limit_m is not None:
        if resource.sea_state_max_m is None or resource.sea_state_max_m < requirement.sea_state_limit_m:
            reasons.append({
                "code": "sea_state_exceeded",
                "message": (
                    f"超出批次可作业浪高：环节要求不高于 {decimal_text(requirement.sea_state_limit_m)} 米，"
                    f"批次上限 {decimal_text(resource.sea_state_max_m)} 米"
                ),
            })
    if resource.available_from is not None and resource.available_to is not None:
        if not windows_overlap(operation_window, (resource.available_from, resource.available_to)):
            reasons.append({
                "code": "window_unavailable",
                "message": "批次可用时间窗与作业海况窗口不重叠",
            })
    if overlapping_usage + 1 > resource.capacity:
        reasons.append({
            "code": "capacity_unavailable",
            "message": f"海况窗口内容量不足：已占用 {overlapping_usage}，批次容量 {resource.capacity}",
        })
    return reasons


def _overlapping_usage_count(
    usages: Mapping[str, Sequence[tuple[str, str]]],
    resource_id: str,
    window: tuple[str, str],
) -> int:
    return sum(1 for used_window in usages.get(resource_id, ()) if windows_overlap(window, used_window))


@dataclass(frozen=True, slots=True)
class Option:
    resource_id: str
    batch_id: str | None
    rank: int  # 在降级顺序中的位置，0 为最优
    reasons: tuple[dict[str, str], ...]


def evaluate_requirement(
    operation: MaintenanceOperation,
    requirement: OperationRequirement,
    resources: Mapping[str, ResourceSnapshot],
    external_usages: Mapping[str, Sequence[tuple[str, str]]],
) -> list[Option]:
    """按降级顺序评估每个备选资源，返回逐项判定（含未满足原因）。"""
    window = (operation.sea_window.starts_at, operation.sea_window.ends_at)
    options: list[Option] = []
    for rank, resource_id in enumerate(requirement.preference_order):
        resource = resources.get(resource_id)
        overlapping = 0 if resource is None else _overlapping_usage_count(
            external_usages, resource_id, window
        )
        reasons = _eligibility_reasons(
            requirement, resource, operation_window=window, overlapping_usage=overlapping
        )
        options.append(Option(
            resource_id=resource_id,
            batch_id=None if resource is None else resource.batch_id,
            rank=rank,
            reasons=tuple(reasons),
        ))
    return options


def operation_combinations(
    operation: MaintenanceOperation,
    resources: Mapping[str, ResourceSnapshot],
    external_usages: Mapping[str, Sequence[tuple[str, str]]],
    pinned_choice: Mapping[str, tuple[str, str | None, int]] | None = None,
) -> dict[str, Any]:
    """评估单个作业的所有环节并枚举降级组合。

    pinned_choice 非空时表示作业已经开工：每个环节的资源被固定为既有占用，
    不参与替代枚举（已经开工的作业不能被自动迁移），但仍展示各偏好的判定。
    """
    window = (operation.sea_window.starts_at, operation.sea_window.ends_at)
    requirement_views: list[dict[str, Any]] = []
    choice_columns: list[list[Option | None]] = []
    blocked: list[dict[str, str]] = []
    for requirement in operation.requirements:
        evaluated = evaluate_requirement(operation, requirement, resources, external_usages)
        eligible = [option for option in evaluated if not option.reasons]
        requirement_views.append({
            "requirement_id": requirement.requirement_id,
            "kind": requirement.kind,
            "essential": requirement.essential,
            "pinned": pinned_choice is not None,
            "selected_of_preference": None if pinned_choice is None else pinned_choice.get(requirement.requirement_id, (None, None, None))[2],
            "evaluation": [
                {
                    "resource_id": option.resource_id,
                    "batch_id": option.batch_id,
                    "preference_rank": option.rank,
                    "eligible": not option.reasons,
                    "unmet_conditions": [dict(reason) for reason in option.reasons],
                }
                for option in evaluated
            ],
        })
        if pinned_choice is not None:
            fixed = pinned_choice.get(requirement.requirement_id)
            if fixed is None:
                if requirement.essential:
                    blocked.append({
                        "requirement_id": requirement.requirement_id,
                        "kind": requirement.kind,
                        "message": "已开工作业缺少该必要环节的既有占用记录",
                    })
                choice_columns.append([None])
            else:
                resource_id, batch_id, rank = fixed
                choice_columns.append([Option(resource_id, batch_id, rank, ())])
            continue
        column: list[Option | None] = eligible[:MAX_RANK_OPTIONS]
        if not requirement.essential:
            # 非必要环节允许整体降级省略，省略排在所有可占用资源之后。
            column.append(None)
        if not column:
            blocked.append({
                "requirement_id": requirement.requirement_id,
                "kind": requirement.kind,
                "message": "必要环节无可用资源，作业不可编排",
            })
        choice_columns.append(column)

    combinations: list[dict[str, Any]] = []
    if not blocked:
        for columns in product(*choice_columns):
            omissions = [
                operation.requirements[index].requirement_id
                for index, option in enumerate(columns)
                if option is None
            ]
            rank_sum = sum(option.rank for option in columns if option is not None)
            choice: dict[str, dict[str, Any]] = {}
            for index, option in enumerate(columns):
                requirement = operation.requirements[index]
                if option is None:
                    continue
                choice[requirement.requirement_id] = {
                    "kind": requirement.kind,
                    "essential": requirement.essential,
                    "resource_id": option.resource_id,
                    "batch_id": option.batch_id,
                    "preference_rank": option.rank,
                }
            combinations.append({
                "choice": choice,
                "omitted": omissions,
                "degradation_level": len(omissions),
                "preference_rank_sum": rank_sum,
                "tiebreaker": tuple(
                    option.resource_id if option is not None else "~omit~" for option in columns
                ),
            })
        combinations.sort(key=lambda item: (
            item["degradation_level"], item["preference_rank_sum"], item["tiebreaker"]
        ))
        combinations = combinations[:MAX_OPERATION_COMBINATIONS]
        for ordinal, item in enumerate(combinations, start=1):
            item["ordinal"] = ordinal
            item.pop("tiebreaker", None)
            item["tradeoffs"] = _combination_tradeoffs(operation, item)
    return {
        "operation_id": operation.operation_id,
        "turbine_model": operation.turbine_model,
        "sea_window": {"starts_at": operation.sea_window.starts_at, "ends_at": operation.sea_window.ends_at},
        "feasible": not blocked,
        "blockers": blocked,
        "requirements": requirement_views,
        "combinations": combinations,
        "_window": window,
    }


def _combination_tradeoffs(operation: MaintenanceOperation, combination: Mapping[str, Any]) -> list[str]:
    messages: list[str] = []
    requirements = {item.requirement_id: item for item in operation.requirements}
    for requirement_id, selected in combination["choice"].items():
        requirement = requirements[requirement_id]
        if selected["preference_rank"] > 0:
            messages.append(
                f"{requirement.kind} 环节降级为第 {selected['preference_rank'] + 1} 偏好 "
                f"{selected['resource_id']}（批次 {selected['batch_id']}）"
            )
    for omitted_id in combination["omitted"]:
        messages.append(f"{requirements[omitted_id].kind} 环节按降级顺序整体省略（非必要环节）")
    return messages


def plan_candidates(
    operations: Sequence[MaintenanceOperation],
    resources: Mapping[str, ResourceSnapshot],
    external_usages: Mapping[str, Sequence[tuple[str, str]]],
    pinned: Mapping[str, Mapping[str, tuple[str, str | None, int]]] | None = None,
) -> dict[str, Any]:
    """枚举整个冻结计划的候选组合，并做跨作业窗口容量校验。

    pinned 给出已开工作业各环节既有的（资源、批次、偏好位次），这些环节固定
    不迁移；external_usages 只包含计划外的其他订单占用，开工固定占用通过
    pinned 单独进入容量校验，避免重复计数。
    """
    pinned = pinned or {}
    operation_views = [
        operation_combinations(
            operation, resources, external_usages, pinned.get(operation.operation_id)
        )
        for operation in operations
    ]
    windows = {view["operation_id"]: view["_window"] for view in operation_views}
    feasible_plan = all(view["feasible"] for view in operation_views)
    candidates: list[dict[str, Any]] = []
    if feasible_plan:
        combo_lists = [view["combinations"] for view in operation_views]
        for joined in product(*combo_lists):
            selections: dict[str, dict[str, dict[str, Any]]] = {}
            omissions: list[str] = []
            tradeoffs: list[str] = []
            degradation = 0
            rank_sum = 0
            usages: dict[str, list[tuple[str, str]]] = {}
            feasible = True
            conflicts: list[str] = []
            for operation, combo in zip(operations, joined):
                selections[operation.operation_id] = combo["choice"]
                omissions.extend(
                    f"{operation.operation_id}:{requirement_id}" for requirement_id in combo["omitted"]
                )
                if operation.operation_id not in pinned:
                    tradeoffs.extend(
                        f"[{operation.operation_id}] {message}" for message in combo["tradeoffs"]
                    )
                degradation += combo["degradation_level"]
                rank_sum += combo["preference_rank_sum"]
                window = windows[operation.operation_id]
                for chosen in combo["choice"].values():
                    usages.setdefault(chosen["resource_id"], []).append(window)
            for resource_id, used_windows in usages.items():
                capacity = resources[resource_id].capacity
                for index, window in enumerate(used_windows):
                    overlapping = sum(
                        1 for other_index, other in enumerate(used_windows)
                        if other_index != index and windows_overlap(window, other)
                    )
                    external = _overlapping_usage_count(external_usages, resource_id, window)
                    if overlapping + external + 1 > capacity:
                        feasible = False
                        conflicts.append(
                            f"资源 {resource_id} 在海况窗口内超出批次容量 {capacity}"
                        )
            tiebreaker = tuple(
                f"{operation.operation_id}:{requirement_id}:{chosen['resource_id']}"
                for operation, combo in zip(operations, joined)
                for requirement_id, chosen in combo["choice"].items()
            )
            candidates.append({
                "selections": selections,
                "omitted": omissions,
                "degradation_level": degradation,
                "preference_rank_sum": rank_sum,
                "feasible": feasible,
                "conflicts": sorted(set(conflicts)),
                "tradeoffs": tradeoffs,
                "tiebreaker": tiebreaker,
            })
        candidates.sort(key=lambda item: (
            0 if item["feasible"] else 1,
            item["degradation_level"],
            item["preference_rank_sum"],
            item["tiebreaker"],
        ))
        candidates = candidates[:MAX_PLAN_CANDIDATES]
        for ordinal, item in enumerate(candidates, start=1):
            item["ordinal"] = ordinal
            item.pop("tiebreaker", None)
    for view in operation_views:
        view.pop("_window", None)
    return {
        "feasible": feasible_plan and any(item["feasible"] for item in candidates),
        "operations": operation_views,
        "candidates": candidates,
        "windows": {key: {"starts_at": value[0], "ends_at": value[1]} for key, value in windows.items()},
    }
