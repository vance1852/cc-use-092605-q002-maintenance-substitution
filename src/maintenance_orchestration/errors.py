"""检修编排服务向 API 和 CLI 暴露的稳定错误。"""


class MaintenanceError(RuntimeError):
    code = "maintenance_error"
    status = 400


class NotFound(MaintenanceError):
    code = "not_found"
    status = 404


class Conflict(MaintenanceError):
    code = "conflict"
    status = 409


class Forbidden(MaintenanceError):
    code = "forbidden"
    status = 403


class InvalidState(MaintenanceError):
    code = "invalid_state"
    status = 409


class ValidationFailed(MaintenanceError):
    code = "validation_failed"
    status = 422
