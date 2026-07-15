"""Uniform API error responses with stable codes and request correlation."""

from __future__ import annotations

from contextlib import suppress

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from graphrag.api.schemas import ErrorBody
from graphrag.domain.errors import AppError, ErrorCode
from graphrag.observability.logging import get_logger
from graphrag.observability.redaction import redact_text


def _request_id(request: Request) -> str:
    return str(getattr(request.state, "request_id", "unknown"))


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, error: AppError) -> JSONResponse:
        with suppress(Exception):
            await request.app.state.runtime.audit.record(
                "api.error",
                {
                    "request_id": _request_id(request),
                    "error_code": error.code.value,
                    "status_code": error.status_code,
                    "dependency": error.dependency,
                },
            )
        body = ErrorBody(
            error_code=error.code.value,
            message=redact_text(error.message),
            request_id=_request_id(request),
            retryable=error.retryable,
        )
        headers = {"WWW-Authenticate": "Bearer"} if error.status_code == 401 else None
        return JSONResponse(
            status_code=error.status_code,
            content=body.model_dump(mode="json"),
            headers=headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, _error: RequestValidationError
    ) -> JSONResponse:
        body = ErrorBody(
            error_code=ErrorCode.VALIDATION.value,
            message="请求参数不符合接口契约",
            request_id=_request_id(request),
        )
        return JSONResponse(status_code=422, content=body.model_dump(mode="json"))

    @app.exception_handler(Exception)
    async def internal_error_handler(request: Request, error: Exception) -> JSONResponse:
        get_logger().exception(
            "unhandled_api_error",
            request_id=_request_id(request),
            error_type=type(error).__name__,
        )
        body = ErrorBody(
            error_code=ErrorCode.INTERNAL.value,
            message="服务内部错误",
            request_id=_request_id(request),
            retryable=False,
        )
        return JSONResponse(status_code=500, content=body.model_dump(mode="json"))
