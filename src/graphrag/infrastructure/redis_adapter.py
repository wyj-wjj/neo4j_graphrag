"""Redis checkpoint, cache, lock, rate-limit and readiness adapter."""

from __future__ import annotations

import json
from collections.abc import Awaitable
from typing import Any, cast

from redis.asyncio import Redis

from graphrag.domain.errors import DependencyError
from graphrag.domain.state import AgentState
from graphrag.domain.state_migrations import StateMigrationRegistry


class RedisAdapter:
    _RELEASE_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
      return redis.call('del', KEYS[1])
    end
    return 0
    """

    def __init__(self, url: str) -> None:
        self.client = Redis.from_url(url, decode_responses=True)

    @staticmethod
    def _checkpoint_key(tenant_id: str, session_id: str) -> str:
        return f"agent:{tenant_id}:{session_id}:state"

    async def verify_capabilities(self) -> dict[str, Any]:
        try:
            info = await self.client.info("server")
            modules = await self.client.module_list()
        except Exception as exc:
            raise DependencyError("redis", "Redis 无法连接") from exc
        major = int(str(info.get("redis_version", "0")).split(".")[0])
        names = {str(item.get("name", "")).lower() for item in modules}
        if major < 8 and not {"rejson", "search"}.issubset(names):
            raise DependencyError(
                "redis", "Redis Checkpointer 需要 Redis 8 或 RedisJSON/RediSearch"
            )
        return {"version": info.get("redis_version"), "modules": sorted(names)}

    async def put(self, state: AgentState, *, ttl_seconds: int) -> None:
        await self.client.set(
            self._checkpoint_key(state.tenant_id, state.session_id),
            state.model_dump_json(),
            ex=ttl_seconds,
        )

    async def get(self, tenant_id: str, session_id: str) -> AgentState | None:
        raw = await self.client.get(self._checkpoint_key(tenant_id, session_id))
        if not raw:
            return None
        state = StateMigrationRegistry().load_json(raw)
        if state.tenant_id != tenant_id or state.session_id != session_id:
            raise DependencyError(
                "redis",
                "Checkpoint 身份边界不匹配，已拒绝恢复",
                retryable=False,
            )
        return state

    async def delete(self, tenant_id: str, session_id: str) -> None:
        await self.client.delete(self._checkpoint_key(tenant_id, session_id))

    async def cache_get(self, key: str) -> Any | None:
        raw = await self.client.get(key)
        return json.loads(raw) if raw else None

    async def cache_set(self, key: str, value: Any, *, ttl_seconds: int) -> None:
        await self.client.set(key, json.dumps(value, ensure_ascii=False), ex=ttl_seconds)

    async def acquire_lock(self, key: str, owner: str, *, lease_seconds: int) -> bool:
        return bool(await self.client.set(key, owner, nx=True, ex=lease_seconds))

    async def release_lock(self, key: str, owner: str) -> bool:
        operation = cast(Awaitable[Any], self.client.eval(self._RELEASE_SCRIPT, 1, key, owner))
        return bool(await operation)

    async def allow(self, key: str, *, limit: int, window_seconds: int) -> bool:
        pipe = self.client.pipeline(transaction=True)
        pipe.incr(key)
        pipe.expire(key, window_seconds, nx=True)
        count, _ = await pipe.execute()
        return int(count) <= limit

    async def close(self) -> None:
        await self.client.aclose()
