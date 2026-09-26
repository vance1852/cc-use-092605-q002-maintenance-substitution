"""贯通资源登记、冻结计划、替代候选、原子占用与取消释放的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import Conflict
from .service import MaintenanceOrchestrationService


def _vessel(resource_id: str, batch_id: str, capability: str, sea_state: str) -> dict[str, object]:
    return {
        "resource_id": resource_id, "kind": "vessel", "batch_id": batch_id,
        "capability_mw": capability, "sea_state_max_m": sea_state,
    }


def _window(resource_id: str, batch_id: str, capability: str) -> dict[str, object]:
    return {
        "resource_id": resource_id, "kind": "lifting_window", "batch_id": batch_id,
        "capability_mw": capability,
    }


def _kit(resource_id: str, batch_id: str, models: list[str]) -> dict[str, object]:
    return {"resource_id": resource_id, "kind": "spare_kit", "batch_id": batch_id, "compat_models": models}


def _crew(resource_id: str, batch_id: str, certifications: list[str]) -> dict[str, object]:
    return {"resource_id": resource_id, "kind": "crew", "batch_id": batch_id, "certifications": certifications}


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = MaintenanceOrchestrationService(
        connection, FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
    )
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)

    # 18 兆瓦机组集中检修：母船、吊装窗口、备件兼容范围、具备资质的班组各两档。
    for resource in (
        _vessel("vessel-jingtan", "VES-BATCH-A", "22", "2.5"),
        _vessel("vessel-beihai", "VES-BATCH-B", "18", "2.0"),
        _window("window-am", "WIN-BATCH-AM", "20"),
        _window("window-pm", "WIN-BATCH-PM", "18"),
        _kit("kit-full", "KIT-BATCH-A", ["WT-18MW", "WT-16MW"]),
        _kit("kit-narrow", "KIT-BATCH-B", ["WT-16MW"]),
        _crew("crew-senior", "CREW-BATCH-A", ["GWO", "HOIST-18", "TORQUE-18"]),
        _crew("crew-junior", "CREW-BATCH-B", ["GWO"]),
    ):
        service.register_resource("plan", resource)
    # 主力母船同一海况窗口只能服务一组作业，构成需要替代编排的瓶颈；
    # 吊装窗口、备件包与高级班组容量为 2，可同时保障两组作业。
    service.update_resource("plan", "vessel-jingtan", {"capacity": 1})
    service.update_resource("plan", "window-am", {"capacity": 2})
    service.update_resource("plan", "kit-full", {"capacity": 2})
    service.update_resource("plan", "crew-senior", {"capacity": 2})

    sea_window = {"starts_at": "2026-09-28T02:00:00Z", "ends_at": "2026-09-28T08:00:00Z"}

    def operation(operation_id: str, depends_on: list[str]) -> dict[str, object]:
        prefix = operation_id + "-"
        return {
            "operation_id": operation_id,
            "turbine_model": "WT-18MW",
            "depends_on": depends_on,
            "sea_window": sea_window,
            "requirements": [
                {"requirement_id": prefix + "vessel", "kind": "vessel",
                 "minimum_capability_mw": "18", "sea_state_limit_m": "2.0",
                 "preference_order": ["vessel-jingtan", "vessel-beihai"]},
                {"requirement_id": prefix + "window", "kind": "lifting_window",
                 "minimum_capability_mw": "18",
                 "preference_order": ["window-am", "window-pm"]},
                {"requirement_id": prefix + "kit", "kind": "spare_kit",
                 "required_model": "WT-18MW",
                 "preference_order": ["kit-full", "kit-narrow"]},
                {"requirement_id": prefix + "crew", "kind": "crew",
                 "minimum_certification": "HOIST-18",
                 "preference_order": ["crew-senior", "crew-junior"],
                 "essential": False},
            ],
        }

    service.register_operation("plan", operation("op-foundation", []))
    service.register_operation("plan", operation("op-nacelle", ["op-foundation"]))
    service.freeze_plan("plan", {"plan_id": "plan-18mw-overhaul", "operation_ids": ["op-foundation", "op-nacelle"]})

    first_run = service.generate_candidates("plan", "plan-18mw-overhaul")
    # 首选组合因主力母船容量冲突不可行，系统自动给出母船降级的可执行组合并说明取舍。
    infeasible = [item for item in first_run["candidates"] if not item["feasible"]]
    chosen = next(item for item in first_run["candidates"] if item["feasible"])
    # 一张订单在单事务内原子占用两个作业的全部关联资源。
    first_order = service.confirm_order("dispatch", {
        "plan_id": "plan-18mw-overhaul",
        "order_id": "order-overhaul",
        "candidate_run_id": first_run["run_id"],
        "candidate_ordinal": chosen["ordinal"],
        "idempotency_key": "confirm-overhaul-0001",
    })
    reservation_count = len(first_order["reservations"])
    # 相同业务请求重放不重复占用。
    replayed = service.confirm_order("dispatch", {
        "plan_id": "plan-18mw-overhaul",
        "order_id": "order-overhaul",
        "candidate_run_id": first_run["run_id"],
        "candidate_ordinal": chosen["ordinal"],
        "idempotency_key": "confirm-overhaul-0001",
    })
    replay_stable = (
        replayed["order_id"] == first_order["order_id"]
        and connection.execute("SELECT count(*) FROM resource_reservations").fetchone()[0] == reservation_count
    )

    # 每项占用可追溯资源批次与冻结时的资源修订。
    batch_trace = sorted({(item["resource_id"], item["batch_id"]) for item in first_order["reservations"]})

    # 基础作业开工：其预留转为已消耗，后续候选固定不迁移。
    service.start_operation("dispatch", "op-foundation")
    # 取消整单只释放尚未消耗（机舱作业）的预留，基础作业消耗保留。
    cancelled = service.cancel_order("dispatch", "order-overhaul", "气象更新，机舱作业暂缓")
    foundation_states = {
        item["state"] for item in cancelled["reservations"] if item["operation_id"] == "op-foundation"
    }
    nacelle_states = {
        item["state"] for item in cancelled["reservations"] if item["operation_id"] == "op-nacelle"
    }

    # 重新生成候选：已开工的基础作业固定，机舱作业重新参与编排。
    second_run = service.generate_candidates("plan", "plan-18mw-overhaul")
    pinned = [item["operation_id"] for item in second_run["pinned_operations"]]
    fresh_choice = next(item for item in second_run["candidates"] if item["feasible"])

    # 机舱作业确认前，其候选选用的母船批次临时变化（版本递增），旧候选整单确认失败。
    nacelle_selection = fresh_choice["selections"]["op-nacelle"]
    revised_resource = next(
        chosen["resource_id"] for chosen in nacelle_selection.values() if chosen["kind"] == "vessel"
    )
    service.update_resource("plan", revised_resource, {"note": "母船船期临时调整"})
    version_rejected = False
    try:
        service.confirm_order("dispatch", {
            "plan_id": "plan-18mw-overhaul",
            "order_id": "order-nacelle-stale",
            "candidate_run_id": second_run["run_id"],
            "candidate_ordinal": fresh_choice["ordinal"],
            "idempotency_key": "confirm-nacelle-stale",
        })
    except Conflict:
        version_rejected = True

    # 基于新版本重新生成后确认成功，只占用机舱作业（基础作业保持固定）。
    third_run = service.generate_candidates("plan", "plan-18mw-overhaul")
    third_choice = next(item for item in third_run["candidates"] if item["feasible"])
    second_order = service.confirm_order("dispatch", {
        "plan_id": "plan-18mw-overhaul",
        "order_id": "order-nacelle",
        "candidate_run_id": third_run["run_id"],
        "candidate_ordinal": third_choice["ordinal"],
        "idempotency_key": "confirm-nacelle-0001",
    })

    audit = service.audit_chain("audit")
    result = {
        "status": "ok",
        "workspace": workspace.name,
        "first_run_id": first_run["run_id"],
        "candidate_count": len(first_run["candidates"]),
        "infeasible_conflicts": infeasible[0]["conflicts"],
        "chosen_ordinal": chosen["ordinal"],
        "chosen_tradeoffs": chosen["tradeoffs"],
        "atomic_reservations": reservation_count,
        "replay_did_not_double_occupy": replay_stable,
        "resource_batch_trace": [list(item) for item in batch_trace],
        "cancel_kept_consumed": sorted(foundation_states),
        "cancel_released_unconsumed": sorted(nacelle_states),
        "pinned_after_start": pinned,
        "stale_version_confirm_rejected": version_rejected,
        "second_order_reservations": len(second_order["reservations"]),
        "audit": audit,
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
