"""检修资源替代编排的领域输入契约。

计划人员登记四类可替代资源（母船、吊装窗口、备件包、班组）以及作业的
作业依赖、海况窗口、最低资质、备件兼容范围和可接受的降级顺序。所有契约
均为不可变值对象，解析时完成结构和取值范围校验。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .clock import parse_utc
from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")

RESOURCE_KINDS = {"vessel", "lifting_window", "spare_kit", "crew"}
OPERATION_STATES = ("registered", "started", "completed", "cancelled")


def required_text(value: object, field_name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field_name} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field_name} 不能超过 {maximum} 个字符")
    return result


def identifier(value: object, field_name: str) -> str:
    result = required_text(value, field_name, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field_name} 格式不正确")
    return result


def resource_kind(value: object, field_name: str = "kind") -> str:
    result = required_text(value, field_name, 32)
    if result not in RESOURCE_KINDS:
        raise ValidationFailed(f"{field_name} 必须是 vessel、lifting_window、spare_kit 或 crew")
    return result


def decimal_value(
    value: object,
    field_name: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if isinstance(value, bool):
        raise ValidationFailed(f"{field_name} 必须是数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationFailed(f"{field_name} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationFailed(f"{field_name} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationFailed(f"{field_name} 不能小于 {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationFailed(f"{field_name} 不能大于 {maximum}")
    return result


def nonnegative_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationFailed(f"{field_name} 必须是非负整数")
    return value


def positive_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationFailed(f"{field_name} 必须是正整数")
    return value


def _string_set(value: object, field_name: str) -> frozenset[str]:
    if not isinstance(value, (list, tuple, set)) or not value:
        raise ValidationFailed(f"{field_name} 必须是非空字符串数组")
    result: set[str] = set()
    for item in value:
        text = required_text(item, f"{field_name} 元素", 64)
        result.add(text)
    return frozenset(result)


def _time_window(raw: Mapping[str, Any], field_name: str) -> "TimeWindow":
    starts_at = required_text(raw.get("starts_at"), f"{field_name}.starts_at", 40)
    ends_at = required_text(raw.get("ends_at"), f"{field_name}.ends_at", 40)
    try:
        start = parse_utc(starts_at, f"{field_name}.starts_at")
        end = parse_utc(ends_at, f"{field_name}.ends_at")
    except ValueError as exc:
        raise ValidationFailed(str(exc)) from exc
    if end <= start:
        raise ValidationFailed(f"{field_name}.ends_at 必须晚于 starts_at")
    return TimeWindow(starts_at=starts_at, ends_at=ends_at)


@dataclass(frozen=True, slots=True)
class TimeWindow:
    starts_at: str
    ends_at: str


@dataclass(frozen=True, slots=True)
class ResourceCandidate:
    """可参与编排的资源批次。

    capability 记录能力维度：母船/吊装窗口为可承载的最大机组兆瓦数；
    备件包为兼容机型集合（存于 compat_models）；班组为具备的资质集合
    （存于 certifications）。sea_state_max 是该批次可作业的最大浪高（米）。
    """

    resource_id: str
    kind: str
    batch_id: str
    capability_mw: Decimal | None
    sea_state_max_m: Decimal | None
    available_from: str | None
    available_to: str | None
    certifications: frozenset[str] = field(default_factory=frozenset)
    compat_models: frozenset[str] = field(default_factory=frozenset)
    note: str = ""

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ResourceCandidate":
        kind = resource_kind(raw.get("kind"))
        capability = raw.get("capability_mw")
        sea_state = raw.get("sea_state_max_m")
        certifications = _string_set(raw["certifications"], "certifications") if kind == "crew" else frozenset()
        compat_models = _string_set(raw["compat_models"], "compat_models") if kind == "spare_kit" else frozenset()
        if kind != "crew" and "certifications" in raw:
            raise ValidationFailed("仅班组资源可以登记 certifications")
        if kind != "spare_kit" and "compat_models" in raw:
            raise ValidationFailed("仅备件包可以登记 compat_models")
        if kind in {"vessel", "lifting_window"} and capability is None:
            raise ValidationFailed(f"{kind} 资源必须登记 capability_mw")
        window_start = raw.get("available_from")
        window_end = raw.get("available_to")
        if (window_start is None) != (window_end is None):
            raise ValidationFailed("available_from 与 available_to 必须同时提供")
        if window_start is not None and window_end is not None:
            _time_window({"starts_at": window_start, "ends_at": window_end}, "可用窗口")
        note = raw.get("note", "")
        if note is None:
            note = ""
        if not isinstance(note, str):
            raise ValidationFailed("note 必须是字符串")
        note = note.strip()
        if len(note) > 256:
            raise ValidationFailed("note 不能超过 256 个字符")
        return cls(
            resource_id=identifier(raw.get("resource_id"), "resource_id"),
            kind=kind,
            batch_id=identifier(raw.get("batch_id"), "batch_id"),
            capability_mw=None if capability is None else decimal_value(
                capability, "capability_mw", minimum=Decimal("0")
            ),
            sea_state_max_m=None if sea_state is None else decimal_value(
                sea_state, "sea_state_max_m", minimum=Decimal("0")
            ),
            available_from=window_start,
            available_to=window_end,
            certifications=certifications,
            compat_models=compat_models,
            note=note,
        )


@dataclass(frozen=True, slots=True)
class OperationRequirement:
    """单个作业环节对一类资源的最低要求。

    preference_order 是可接受的降级顺序：计划人员按优先级列出资源编号，
    越靠前越优先；列表之外的资源即使硬性条件满足也不参与候选。
    """

    requirement_id: str
    kind: str
    minimum_certification: str | None
    minimum_capability_mw: Decimal | None
    required_model: str | None
    sea_state_limit_m: Decimal | None
    preference_order: tuple[str, ...]
    essential: bool

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "OperationRequirement":
        kind = resource_kind(raw.get("kind"))
        order = raw.get("preference_order", [])
        if not isinstance(order, list) or not order:
            raise ValidationFailed("preference_order 必须是非空资源编号数组")
        preferences: list[str] = []
        for item in order:
            resource_id = identifier(item, "preference_order 元素")
            if resource_id in preferences:
                raise ValidationFailed("preference_order 不能包含重复资源")
            preferences.append(resource_id)
        minimum_cert = raw.get("minimum_certification")
        required_model = raw.get("required_model")
        if kind != "crew" and minimum_cert is not None:
            raise ValidationFailed("仅班组环节可以设置 minimum_certification")
        if kind == "crew":
            minimum_cert = required_text(minimum_cert, "minimum_certification", 64)
        if kind != "spare_kit" and required_model is not None:
            raise ValidationFailed("仅备件包环节可以设置 required_model")
        capability = raw.get("minimum_capability_mw")
        if kind in {"vessel", "lifting_window"} and capability is None:
            raise ValidationFailed(f"{kind} 环节必须设置 minimum_capability_mw")
        if kind not in {"vessel", "lifting_window"} and capability is not None:
            raise ValidationFailed("仅母船和吊装窗口环节可以设置 minimum_capability_mw")
        sea_state = raw.get("sea_state_limit_m")
        essential = raw.get("essential", True)
        if not isinstance(essential, bool):
            raise ValidationFailed("essential 必须是布尔值")
        return cls(
            requirement_id=identifier(raw.get("requirement_id"), "requirement_id"),
            kind=kind,
            minimum_certification=minimum_cert,
            minimum_capability_mw=None if capability is None else decimal_value(
                capability, "minimum_capability_mw", minimum=Decimal("0")
            ),
            required_model=None if required_model is None else required_text(required_model, "required_model", 64),
            sea_state_limit_m=None if sea_state is None else decimal_value(
                sea_state, "sea_state_limit_m", minimum=Decimal("0")
            ),
            preference_order=tuple(preferences),
            essential=bool(raw.get("essential", True)),
        )


@dataclass(frozen=True, slots=True)
class MaintenanceOperation:
    """集中检修作业：依赖、海况窗口与各环节资源要求。"""

    operation_id: str
    turbine_model: str
    sea_window: TimeWindow
    requirements: tuple[OperationRequirement, ...]
    depends_on: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "MaintenanceOperation":
        requirements_raw = raw.get("requirements", [])
        if not isinstance(requirements_raw, list) or not requirements_raw:
            raise ValidationFailed("requirements 必须是非空数组")
        requirements = tuple(OperationRequirement.from_dict(item) for item in requirements_raw)
        requirement_ids = [item.requirement_id for item in requirements]
        if len(set(requirement_ids)) != len(requirement_ids):
            raise ValidationFailed("requirement_id 不能重复")
        kinds = [item.kind for item in requirements]
        if len(set(kinds)) != len(kinds):
            raise ValidationFailed("同一作业内每类资源只能有一条要求")
        dependencies = raw.get("depends_on", [])
        if not isinstance(dependencies, list):
            raise ValidationFailed("depends_on 必须是数组")
        depends_on = frozenset(identifier(item, "depends_on 元素") for item in dependencies)
        operation_id = identifier(raw.get("operation_id"), "operation_id")
        if operation_id in depends_on:
            raise ValidationFailed("作业不能依赖自身")
        sea_window_raw = raw.get("sea_window")
        if not isinstance(sea_window_raw, Mapping):
            raise ValidationFailed("sea_window 必须是包含 starts_at 和 ends_at 的对象")
        return cls(
            operation_id=operation_id,
            turbine_model=required_text(raw.get("turbine_model"), "turbine_model", 64),
            sea_window=_time_window(sea_window_raw, "sea_window"),
            requirements=requirements,
            depends_on=depends_on,
        )
