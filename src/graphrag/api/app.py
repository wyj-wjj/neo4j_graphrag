"""Side-effect-free FastAPI factory with explicit lifespan ownership."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response
from starlette.middleware.base import RequestResponseEndpoint

from graphrag.api.auth import JWTAuth
from graphrag.api.errors import install_exception_handlers
from graphrag.api.routes import router
from graphrag.api.schemas import ErrorBody
from graphrag.application.container import Runtime, build_runtime
from graphrag.config import Settings, get_settings
from graphrag.domain.ids import new_id
from graphrag.observability.logging import configure_logging

_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{8,128}$")


def create_app(
    settings: Settings | None = None,
    *,
    runtime_factory: Callable[[Settings], Runtime] = build_runtime,
) -> FastAPI:
    resolved = settings or get_settings()
    configure_logging(resolved.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runtime = runtime_factory(resolved)
        app.state.runtime = runtime
        app.state.auth = JWTAuth(resolved)
        try:
            await runtime.start()
            yield
        finally:
            await runtime.close()

    app = FastAPI(
        title=resolved.app_name,
        version="0.1.0",
        openapi_url="/api/v1/openapi.json",
        docs_url="/api/v1/docs",
        lifespan=lifespan,
        responses={
            code: {"model": ErrorBody, "description": description}
            for code, description in {
                400: "Bad request",
                401: "Authentication failed",
                403: "Authorization denied",
                404: "Resource not found",
                409: "Resource conflict",
                422: "Validation failed",
                429: "Rate limited",
                500: "Internal error",
                503: "Dependency unavailable",
                504: "Operation timeout",
            }.items()
        },
    )

    @app.middleware("http")
    async def request_context(request: Request, call_next: RequestResponseEndpoint) -> Response:
        supplied = request.headers.get("X-Request-ID", "")
        request_id = supplied if _REQUEST_ID.fullmatch(supplied) else new_id()
        request.state.request_id = request_id
        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            response = await call_next(request)
        finally:
            structlog.contextvars.clear_contextvars()
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    install_exception_handlers(app)
    app.include_router(router)
    return app
