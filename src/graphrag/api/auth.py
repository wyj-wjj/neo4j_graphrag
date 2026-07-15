"""JWT verification and dev/test-only local token issuance."""

from __future__ import annotations

import asyncio
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from fastapi import HTTPException, Request
from fastapi.security.utils import get_authorization_scheme_param
from jwt import PyJWKClient

from graphrag.config import AppEnvironment, Settings
from graphrag.domain.errors import AuthenticationError, AuthorizationError
from graphrag.domain.models import IdentityContext, IdentitySource


class JWTAuth:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        configured = settings.jwt_dev_secret
        self._dev_secret = (
            configured.get_secret_value() if configured else secrets.token_urlsafe(48)
        )
        self._jwks = PyJWKClient(str(settings.jwt_jwks_url)) if settings.jwt_jwks_url else None

    def issue_dev_token(
        self, *, user_id: str, roles: frozenset[str], expires_minutes: int
    ) -> tuple[str, int]:
        if self.settings.app_env not in {AppEnvironment.DEV, AppEnvironment.TEST}:
            raise AuthorizationError("生产环境禁止签发本地测试身份")
        now = datetime.now(UTC)
        expires = now + timedelta(minutes=expires_minutes)
        token = jwt.encode(
            {
                "sub": user_id,
                "tenant_id": self.settings.default_tenant_id,
                "roles": sorted(roles),
                "departments": [],
                "iss": self.settings.jwt_issuer,
                "aud": self.settings.jwt_audience,
                "iat": now,
                "exp": expires,
                "identity_source": "dev",
            },
            self._dev_secret,
            algorithm="HS256",
        )
        return token, expires_minutes * 60

    async def verify(self, token: str) -> IdentityContext:
        try:
            if self.settings.app_env in {AppEnvironment.DEV, AppEnvironment.TEST}:
                claims = jwt.decode(
                    token,
                    self._dev_secret,
                    algorithms=["HS256"],
                    issuer=self.settings.jwt_issuer,
                    audience=self.settings.jwt_audience,
                    options={"require": ["exp", "iat", "sub", "tenant_id", "roles"]},
                )
                source = IdentitySource.DEV
            else:
                if self._jwks is None:
                    raise AuthenticationError()
                key = await asyncio.to_thread(self._jwks.get_signing_key_from_jwt, token)
                claims = jwt.decode(
                    token,
                    key.key,
                    algorithms=[self.settings.jwt_algorithm],
                    issuer=self.settings.jwt_issuer,
                    audience=self.settings.jwt_audience,
                    options={"require": ["exp", "iat", "sub", "tenant_id", "roles"]},
                )
                source = IdentitySource.JWKS
        except AuthenticationError:
            raise
        except Exception as exc:
            raise AuthenticationError() from exc
        return self._identity_from_claims(claims, source=source)

    @staticmethod
    def _identity_from_claims(claims: dict[str, Any], *, source: IdentitySource) -> IdentityContext:
        roles = claims.get("roles")
        departments = claims.get("departments", [])
        if not isinstance(roles, list) or not all(isinstance(item, str) for item in roles):
            raise AuthenticationError()
        if not isinstance(departments, list) or not all(
            isinstance(item, str) for item in departments
        ):
            raise AuthenticationError()
        try:
            return IdentityContext(
                tenant_id=str(claims["tenant_id"]),
                user_id=str(claims["sub"]),
                roles=frozenset(roles),
                departments=frozenset(departments),
                source=source,
            )
        except Exception as exc:
            raise AuthenticationError() from exc


async def get_identity(request: Request) -> IdentityContext:
    scheme, token = get_authorization_scheme_param(request.headers.get("Authorization"))
    if scheme.lower() != "bearer" or not token:
        raise AuthenticationError()
    auth: JWTAuth = request.app.state.auth
    return await auth.verify(token)


def require_roles(identity: IdentityContext, *roles: str) -> None:
    if not identity.has_role(*roles):
        raise AuthorizationError()


def authentication_challenge() -> HTTPException:
    return HTTPException(status_code=401, headers={"WWW-Authenticate": "Bearer"})
