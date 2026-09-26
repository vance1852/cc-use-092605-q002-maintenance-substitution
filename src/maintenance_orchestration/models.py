"""检修资源替代编排的领域输入契约。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .clock import parse_utc, utc_text
from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
RESOURCE_KINDS = ("mother-vessel", "lifting-window", "spare-kit", "crew")
VESSEL_KINDS = ("mother-vessel", "lifting-window")
QUALIFICATION_LEVELS = {"C": 1, "B": 2, "A": 3}
BATCH_STATES = ("active", "suspended", "retired")


def required_text(value: object, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum} 个字符")
    return result


def identifier(value: object, field: str) -> str:
    result = required_text(value, field, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field} 格式不正确")
    return result


def decimal_value(
    value: object,
    field: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if isinstance(value, bool):
        raise ValidationFailed(f"{field} 必须是数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationFailed(f"{field} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationFailed(f"{field} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationFailed(f"{field} 不能小于 {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationFailed(f"{field} 不能大于 {maximum}")
    return result


def positive_integer(value: object, field: str, maximum: int = 9999) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationFailed(f"{field} 必须是正整数")
    if value > maximum:
        raise ValidationFailed(f"{field} 不能大于 {maximum}")
    return value


def identifier_tuple(value: object, field: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if value is None and allow_empty:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ValidationFailed(f"{field} 必须是数组")
    items = tuple(identifier(item, f"{field} 元素") for item in value)
    if not items and not allow_empty:
        raise ValidationFailed(f"{field} 不能为空")
    if len(set(items)) != len(items):
        raise ValidationFailed(f"{field} 不能包含重复项")
    return items


def instant_text(value: object, field: str) -> str:
    text = required_text(value, field, 40)
    try:
        return utc_text(parse_utc(text, field))
    except ValueError as exc:
        raise ValidationFailed(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class ResourceBatch:
    batch_id: str
    kind: str
    name: str
    quantity: int
    qualification: str | None
    compatible_parts: tuple[str, ...]
    max_wave_m: Decimal | None
    max_wind_ms: Decimal | None
    window_start: str | None
    window_end: str | None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ResourceBatch":
        kind = required_text(raw.get("kind"), "kind", 32)
        if kind not in RESOURCE_KINDS:
            raise ValidationFailed("kind 不是受支持的资源类型")
        qualification = None
        compatible_parts: tuple[str, ...] = ()
        max_wave_m = None
        max_wind_ms = None
        window_start = None
        window_end = None
        if kind == "crew":
            qualification = required_text(raw.get("qualification"), "qualification", 16).upper()
            if qualification not in QUALIFICATION_LEVELS:
                raise ValidationFailed("qualification 必须是 A、B 或 C")
        if kind == "spare-kit":
            compatible_parts = identifier_tuple(raw.get("compatible_parts"), "compatible_parts")
        if kind in VESSEL_KINDS:
            max_wave_m = decimal_value(
                raw.get("max_wave_m"), "max_wave_m", minimum=Decimal("0.1"), maximum=Decimal("30")
            )
            max_wind_ms = decimal_value(
                raw.get("max_wind_ms"), "max_wind_ms", minimum=Decimal("1"), maximum=Decimal("60")
            )
            window_start = instant_text(raw.get("window_start"), "window_start")
            window_end = instant_text(raw.get("window_end"), "window_end")
            if parse_utc(window_end) <= parse_utc(window_start):
                raise ValidationFailed("window_end 必须晚于 window_start")
        return cls(
            batch_id=identifier(raw.get("batch_id"), "batch_id"),
            kind=kind,
            name=required_text(raw.get("name"), "name"),
            quantity=positive_integer(raw.get("quantity"), "quantity"),
            qualification=qualification,
            compatible_parts=compatible_parts,
            max_wave_m=max_wave_m,
            max_wind_ms=max_wind_ms,
            window_start=window_start,
            window_end=window_end,
        )


@dataclass(frozen=True, slots=True)
class DegradationStep:
    rank: int
    qualification: str
    note: str


@dataclass(frozen=True, slots=True)
class ResourceNeed:
    kind: str
    quantity: int


@dataclass(frozen=True, slots=True)
class MaintenanceJob:
    job_id: str
    turbine_id: str
    title: str
    depends_on: tuple[str, ...]
    window_start: str
    window_end: str
    max_wave_m: Decimal
    max_wind_ms: Decimal
    min_qualification: str
    required_parts: tuple[str, ...]
    degradation: tuple[DegradationStep, ...]
    resource_needs: tuple[ResourceNeed, ...]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "MaintenanceJob":
        job_id = identifier(raw.get("job_id"), "job_id")
        depends_on = identifier_tuple(raw.get("depends_on", []), "depends_on", allow_empty=True)
        if job_id in depends_on:
            raise ValidationFailed("作业不能依赖自身")
        window_start = instant_text(raw.get("window_start"), "window_start")
        window_end = instant_text(raw.get("window_end"), "window_end")
        if parse_utc(window_end) <= parse_utc(window_start):
            raise ValidationFailed("window_end 必须晚于 window_start")
        min_qualification = required_text(raw.get("min_qualification"), "min_qualification", 16).upper()
        if min_qualification not in QUALIFICATION_LEVELS:
            raise ValidationFailed("min_qualification 必须是 A、B 或 C")
        return cls(
            job_id=job_id,
            turbine_id=identifier(raw.get("turbine_id"), "turbine_id"),
            title=required_text(raw.get("title"), "title"),
            depends_on=depends_on,
            window_start=window_start,
            window_end=window_end,
            max_wave_m=decimal_value(
                raw.get("max_wave_m"), "max_wave_m", minimum=Decimal("0.1"), maximum=Decimal("30")
            ),
            max_wind_ms=decimal_value(
                raw.get("max_wind_ms"), "max_wind_ms", minimum=Decimal("1"), maximum=Decimal("60")
            ),
            min_qualification=min_qualification,
            required_parts=identifier_tuple(raw.get("required_parts", []), "required_parts", allow_empty=True),
            degradation=cls._degradation(raw.get("degradation_order"), min_qualification),
            resource_needs=cls._needs(raw.get("resource_needs")),
        )

    @staticmethod
    def _degradation(raw_steps: object, min_qualification: str) -> tuple[DegradationStep, ...]:
        if raw_steps is None:
            return (DegradationStep(1, min_qualification, ""),)
        if not isinstance(raw_steps, (list, tuple)) or not raw_steps:
            raise ValidationFailed("degradation_order 必须是非空数组")
        steps: list[DegradationStep] = []
        for index, item in enumerate(raw_steps, start=1):
            if not isinstance(item, Mapping):
                raise ValidationFailed("degradation_order 元素必须是对象")
            rank = item.get("rank")
            if isinstance(rank, bool) or rank != index:
                raise ValidationFailed("degradation_order 的 rank 必须从 1 开始连续递增")
            qualification = required_text(item.get("qualification"), "degradation_order.qualification", 16).upper()
            if qualification not in QUALIFICATION_LEVELS:
                raise ValidationFailed("degradation_order.qualification 必须是 A、B 或 C")
            if QUALIFICATION_LEVELS[qualification] < QUALIFICATION_LEVELS[min_qualification]:
                raise ValidationFailed("degradation_order 资质不能低于 min_qualification")
            note = item.get("note", "")
            if not isinstance(note, str) or len(note.strip()) > 256:
                raise ValidationFailed("degradation_order.note 不能超过 256 个字符")
            steps.append(DegradationStep(index, qualification, note.strip()))
        if len({step.qualification for step in steps}) != len(steps):
            raise ValidationFailed("degradation_order 资质不能重复")
        return tuple(steps)

    @staticmethod
    def _needs(raw_needs: object) -> tuple[ResourceNeed, ...]:
        if raw_needs is None:
            return tuple(ResourceNeed(kind, 1) for kind in RESOURCE_KINDS)
        if not isinstance(raw_needs, (list, tuple)) or not raw_needs:
            raise ValidationFailed("resource_needs 必须是非空数组")
        needs: list[ResourceNeed] = []
        for item in raw_needs:
            if not isinstance(item, Mapping):
                raise ValidationFailed("resource_needs 元素必须是对象")
            kind = required_text(item.get("kind"), "resource_needs.kind", 32)
            if kind not in RESOURCE_KINDS:
                raise ValidationFailed("resource_needs.kind 不是受支持的资源类型")
            needs.append(ResourceNeed(kind, positive_integer(item.get("quantity"), "resource_needs.quantity", 99)))
        kinds = [need.kind for need in needs]
        if len(set(kinds)) != len(kinds):
            raise ValidationFailed("resource_needs 资源类型不能重复")
        if set(kinds) != set(RESOURCE_KINDS):
            raise ValidationFailed("resource_needs 必须同时包含母船、吊装窗口、备件包和班组")
        return tuple(sorted(needs, key=lambda need: RESOURCE_KINDS.index(need.kind)))


@dataclass(frozen=True, slots=True)
class OrchestrationRequestInput:
    request_id: str
    job_ids: tuple[str, ...]
    idempotency_key: str
    max_candidates: int

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "OrchestrationRequestInput":
        job_ids = identifier_tuple(raw.get("job_ids"), "job_ids")
        if len(job_ids) > 50:
            raise ValidationFailed("job_ids 不能超过 50 个")
        max_candidates = raw.get("max_candidates", 3)
        if isinstance(max_candidates, bool) or not isinstance(max_candidates, int) or not 1 <= max_candidates <= 5:
            raise ValidationFailed("max_candidates 必须是 1 到 5 的整数")
        return cls(
            request_id=identifier(raw.get("request_id"), "request_id"),
            job_ids=job_ids,
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
            max_candidates=max_candidates,
        )
