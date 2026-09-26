"""编排服务向 API 和 CLI 暴露的稳定错误。"""


class OrchestrationError(RuntimeError):
    code = "orchestration_error"
    status = 400


class NotFound(OrchestrationError):
    code = "not_found"
    status = 404


class Conflict(OrchestrationError):
    code = "conflict"
    status = 409


class Forbidden(OrchestrationError):
    code = "forbidden"
    status = 403


class InvalidState(OrchestrationError):
    code = "invalid_state"
    status = 409


class ValidationFailed(OrchestrationError):
    code = "validation_failed"
    status = 422
