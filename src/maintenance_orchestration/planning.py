"""确定性的检修资源候选组合生成与取舍说明。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Mapping, Sequence

from .models import QUALIFICATION_LEVELS, DegradationStep, ResourceNeed


KIND_LABELS = {
    "mother-vessel": "母船",
    "lifting-window": "吊装窗口",
    "spare-kit": "备件包",
    "crew": "班组",
    "dependency": "作业依赖",
}


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def window_text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class JobSpec:
    job_id: str
    window_start: datetime
    window_end: datetime
    max_wave_m: Decimal
    max_wind_ms: Decimal
    min_qualification: str
    required_parts: tuple[str, ...]
    degradation: tuple[DegradationStep, ...]
    needs: tuple[ResourceNeed, ...]


@dataclass(frozen=True, slots=True)
class BatchSpec:
    batch_id: str
    kind: str
    revision: int
    available: int
    qualification: str | None
    compatible_parts: tuple[str, ...]
    max_wave_m: Decimal | None
    max_wind_ms: Decimal | None
    window_start: datetime | None
    window_end: datetime | None


@dataclass(frozen=True, slots=True)
class Candidate:
    assignments: tuple[dict[str, object], ...]
    trade_offs: tuple[dict[str, object], ...]
    unmet: tuple[dict[str, object], ...]
    score: int


def degradation_rank(job: JobSpec, qualification: str) -> int:
    """资质在降级顺序中的位次，未列出的合格资质排在最后。"""
    for step in job.degradation:
        if step.qualification == qualification:
            return step.rank
    return len(job.degradation) + 1


def eligibility_failure(job: JobSpec, batch: BatchSpec) -> str | None:
    """返回批次不满足作业要求的原因，满足时返回 None。"""
    if batch.kind == "crew":
        if QUALIFICATION_LEVELS[batch.qualification] < QUALIFICATION_LEVELS[job.min_qualification]:
            return f"资质 {batch.qualification} 低于最低要求 {job.min_qualification}"
        return None
    if batch.kind == "spare-kit":
        missing = [part for part in job.required_parts if part not in batch.compatible_parts]
        if missing:
            return f"备件包不兼容部件 {','.join(missing)}"
        return None
    if batch.max_wave_m < job.max_wave_m:
        return f"抗浪能力 {decimal_text(batch.max_wave_m)}m 低于作业需求 {decimal_text(job.max_wave_m)}m"
    if batch.max_wind_ms < job.max_wind_ms:
        return f"抗风能力 {decimal_text(batch.max_wind_ms)}m/s 低于作业需求 {decimal_text(job.max_wind_ms)}m/s"
    if batch.window_start > job.window_start or batch.window_end < job.window_end:
        return (
            f"可用窗口 {window_text(batch.window_start)}~{window_text(batch.window_end)} "
            f"未覆盖作业窗口 {window_text(job.window_start)}~{window_text(job.window_end)}"
        )
    return None


def _preference_key(job: JobSpec, batch: BatchSpec, rank: int) -> tuple:
    if batch.kind == "crew":
        return (rank, -QUALIFICATION_LEVELS[batch.qualification], batch.batch_id)
    if batch.kind in ("mother-vessel", "lifting-window"):
        return (batch.window_start, batch.batch_id)
    return (batch.batch_id,)


def generate_candidates(
    jobs: Sequence[JobSpec],
    batches: Sequence[BatchSpec],
    dependency_unmet: Mapping[str, tuple[str, ...]],
    max_candidates: int,
) -> list[Candidate]:
    """基于冻结批次版本生成候选组合，附取舍说明和未满足条件。

    每个候选按确定的偏好顺序为每个作业的每类资源挑选批次；第 k 个候选
    优先尝试偏好列表中第 k 位的批次，以此为计划人员提供可执行的替代方案。
    """
    ordered_jobs = sorted(jobs, key=lambda job: (job.window_start, job.job_id))
    eligible: dict[tuple[str, str], list[tuple[BatchSpec, int]]] = {}
    rejections: dict[tuple[str, str], list[str]] = {}
    for job in ordered_jobs:
        for need in job.needs:
            entries: list[tuple[BatchSpec, int]] = []
            rejects: list[str] = []
            for batch in batches:
                if batch.kind != need.kind:
                    continue
                failure = eligibility_failure(job, batch)
                if failure is None:
                    rank = degradation_rank(job, batch.qualification) if batch.kind == "crew" else 1
                    entries.append((batch, rank))
                else:
                    rejects.append(f"{batch.batch_id}（{failure}）")
            entries.sort(key=lambda item: _preference_key(job, item[0], item[1]))
            eligible[(job.job_id, need.kind)] = entries
            rejections[(job.job_id, need.kind)] = rejects
    candidates: list[Candidate] = []
    seen: set[str] = set()
    for index in range(max_candidates):
        remaining = {batch.batch_id: batch.available for batch in batches}
        assignments: list[dict[str, object]] = []
        trade_offs: list[dict[str, object]] = []
        unmet: list[dict[str, object]] = []
        score = 0
        for job in ordered_jobs:
            for reason in dependency_unmet.get(job.job_id, ()):
                unmet.append({"job_id": job.job_id, "kind": "dependency", "reason": reason})
                score += 100
            for need in job.needs:
                entries = eligible[(job.job_id, need.kind)]
                chosen: tuple[BatchSpec, int] | None = None
                chosen_pos = 0
                if entries:
                    positions = [min(index, len(entries) - 1), *range(len(entries))]
                    for pos in dict.fromkeys(positions):
                        batch, rank = entries[pos]
                        if remaining[batch.batch_id] >= need.quantity:
                            chosen = (batch, rank)
                            chosen_pos = pos
                            break
                if chosen is None:
                    if entries:
                        reason = f"满足条件的{KIND_LABELS[need.kind]}批次剩余可用量不足（需求 {need.quantity}）"
                    else:
                        detail = "；".join(rejections[(job.job_id, need.kind)]) or "没有登记批次"
                        reason = f"没有满足条件的{KIND_LABELS[need.kind]}批次：{detail}"
                    unmet.append({"job_id": job.job_id, "kind": need.kind, "reason": reason})
                    score += 100
                    continue
                batch, rank = chosen
                remaining[batch.batch_id] -= need.quantity
                assignments.append({
                    "job_id": job.job_id,
                    "kind": need.kind,
                    "batch_id": batch.batch_id,
                    "batch_revision": batch.revision,
                    "quantity": need.quantity,
                    "degradation_rank": rank,
                })
                score += (rank - 1) * 10 + chosen_pos
                if need.kind == "crew" and rank > 1:
                    trade_offs.append({
                        "job_id": job.job_id,
                        "kind": need.kind,
                        "batch_id": batch.batch_id,
                        "detail": f"班组资质降级为 {batch.qualification}（首选 {job.degradation[0].qualification}）",
                    })
                if chosen_pos > 0:
                    trade_offs.append({
                        "job_id": job.job_id,
                        "kind": need.kind,
                        "batch_id": batch.batch_id,
                        "detail": f"{KIND_LABELS[need.kind]}改用备选批次 {batch.batch_id}（首选批次剩余不足或已保留）",
                    })
        fingerprint = canonical_json(assignments)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        candidates.append(Candidate(tuple(assignments), tuple(trade_offs), tuple(unmet), score))
    return sorted(candidates, key=lambda item: (item.score, canonical_json(list(item.assignments))))
