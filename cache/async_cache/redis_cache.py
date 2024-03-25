import redis.asyncio as async_redis
import toolbox.config as config
from typing import Any, Optional
from toolbox.cache.utils import get_key


class RedisCache:

    def __init__(self, db_name: str = None) -> None:
        self.db_name = db_name

    async def _ensure_connection(self):
        if not hasattr(RedisCache, 'instance'):
            RedisCache.instance = await async_redis.Redis(
                host=config.redis_service(),
                port=config.redis_port(),
                password='',
                decode_responses=True,
            )

    async def get(self, key: str) -> str:
        await self._ensure_connection()
        return await RedisCache.instance.get(key)

    async def set(
        self,
        key: str,
        value: Any,
        ex: Optional[int],
    ):
        await self._ensure_connection()
        await RedisCache.instance.set(
            key,
            value,
            ex=ex,
        )

    def get_key(self, *args) -> str:
        return get_key(self.db_name, args)
