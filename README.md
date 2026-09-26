# 增加机组检修资源替代编排基础平台

本项目是一套可离线运行的 Python 服务端平台，用于管理海上风电场、送出通道、机组资源批次、场站申报、功率分配、调度情景、机组健康准入，以及 18 兆瓦机组集中检修期间的母船、吊装窗口、备件包与资质班组的替代编排。业务状态、登录权限、幂等结果和审计事件保存在 SQLite 中，适合生产调度、设备质量、风险与审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/wind_dispatch/`：场站、送出通道、机组可用量、功率申报、日前分配和调度情景；
- `src/turbine_health/`：机组健康协议、测点导入、异常复核、分析任务租约和健康决定；
- `src/grid_qualification/`：并网机组批次、检测数据、分析、账号登录与质量审批；
- `src/maintenance_orchestration/`：检修作业依赖、海况窗口、最低资质、备件兼容范围与降级顺序登记，候选组合生成、原子占用、开工固定与取消部分释放；
- `fixtures/`：离线验收使用的检测协议和结构化测点；
- `tests/`：核心规则、权限、错误边界、事务、API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m wind_dispatch.acceptance --workspace .
PYTHONPATH=src python3 -m turbine_health.acceptance --workspace .
PYTHONPATH=src python3 -m grid_qualification.acceptance
PYTHONPATH=src python3 -m maintenance_orchestration.acceptance --workspace .
```

四条命令使用临时 SQLite 数据库完成场站与通道登记、功率申报分配、健康测点分析、并网审批，以及检修资源登记、冻结计划、替代候选、原子占用与取消释放，不访问外部网络。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m wind_dispatch.api --database wind.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m turbine_health.api --database health.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m grid_qualification.api --database grid.sqlite3 --host 127.0.0.1 --port 8082
PYTHONPATH=src python3 -m maintenance_orchestration.api --database maintenance.sqlite3 --host 127.0.0.1 --port 8083
```

服务均提供 `GET /health`，其余接口使用 JSON。账号登录和角色权限由服务端校验，进程重启后可以继续查询 SQLite 中的业务状态与审计历史。

## 检修资源替代编排

计划人员先登记四类资源批次（母船 `vessel`、吊装窗口 `lifting_window`、备件包 `spare_kit`、班组 `crew`，批次含容量、浪高上限、可用时间窗、备件兼容机型与班组资质），再登记检修作业的作业依赖、海况窗口、各环节最低要求与按优先级排列的可接受降级顺序：

- `POST /resources`、`POST /resources/{id}/revisions`：登记资源批次；批次字段变化即递增 `revision`。
- `POST /operations`、`POST /operations/{id}/start`：登记作业（`depends_on`、`sea_window`、`minimum_capability_mw`、`minimum_certification`、`required_model`、`sea_state_limit_m`、`preference_order`、`essential`）或开工。
- `POST /plans`：基于作业当前定义冻结计划版本（相同内容去重，依赖必须先于后继作业）。
- `POST /plans/{id}/candidates`：基于冻结版本与资源当前版本快照生成候选组合。每个偏好资源逐项给出是否可占用及未满足条件（资质缺失、备件不兼容、浪高超限、窗口不重叠、容量不足等）；候选按降级程度和偏好位次排序，瓶颈冲突的首选组合标记为不可行并给出可执行的降级组合及取舍说明。
- `POST /orders`：确认候选。单事务内原子占用全部关联资源；候选快照中任一资源 `revision` 变化，整单确认失败（409）。相同 `idempotency_key` 的重放返回首次结果，不重复占用。
- `POST /orders/{id}/cancel`：只把尚未消耗的 `reserved` 预留置为 `released`；已开工的 `consumed` 预留保留。

已经开工（或已有未取消工单占用）的作业在候选中固定，不参与自动迁移；每张预留记录都带有资源来源批次 `batch_id` 与占用时的 `resource_revision`，`GET /orders/{id}` 可追溯每项资源来自哪个批次，候选结果中保留每个偏好资源未满足的条件。
