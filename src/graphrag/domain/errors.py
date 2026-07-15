"""Stable domain error taxonomy shared by API and adapters."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ErrorCode(StrEnum):
    CONFIGURATION = "configuration_error"
    AUTHENTICATION = "authentication_failed"
    AUTHORIZATION = "authorization_denied"
    VALIDATION = "validation_error"
    DEPENDENCY = "dependency_unavailable"
    TIMEOUT = "operation_timeout"
    CONFLICT = "resource_conflict"
    NOT_FOUND = "resource_not_found"
    RATE_LIMITED = "rate_limited"
    UNSAFE_OPERATION = "unsafe_operation"
    INTERNAL = "internal_error"


@dataclass(slots=True)
class AppError(Exception):
    code: ErrorCode
    message: str
    status_code: int = 500
    dependency: str | None = None
    retryable: bool = False

    def __str__(self) -> str:
        return self.message


class ValidationError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.VALIDATION, message, 422)


class ConfigurationError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.CONFIGURATION, message, 500)


class AuthenticationError(AppError):
    def __init__(self, message: str = "身份验证失败") -> None:
        super().__init__(ErrorCode.AUTHENTICATION, message, 401)


class AuthorizationError(AppError):
    def __init__(self, message: str = "没有执行此操作的权限") -> None:
        super().__init__(ErrorCode.AUTHORIZATION, message, 403)


class NotFoundError(AppError):
    def __init__(self, message: str = "资源不存在") -> None:
        super().__init__(ErrorCode.NOT_FOUND, message, 404)


class ConflictError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.CONFLICT, message, 409)


class DependencyError(AppError):
    def __init__(self, dependency: str, message: str, *, retryable: bool = True) -> None:
        super().__init__(ErrorCode.DEPENDENCY, message, 503, dependency, retryable)


class OperationTimeoutError(AppError):
    def __init__(self, dependency: str) -> None:
        super().__init__(
            ErrorCode.TIMEOUT,
            f"{dependency} 操作超时",
            504,
            dependency,
            True,
        )


class UnsafeOperationError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.UNSAFE_OPERATION, message, 403)


class RateLimitError(AppError):
    def __init__(self, message: str = "请求过于频繁") -> None:
        super().__init__(ErrorCode.RATE_LIMITED, message, 429, retryable=True)
