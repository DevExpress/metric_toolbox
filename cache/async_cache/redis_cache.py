import redis.asyncio as async_redis
import toolbox.config as config
from typing import Any, Optional
from toolbox.cache.utils import get_key as _get_key


__instance = async_redis.Redis(
    host=config.redis_service(),
    port=config.redis_port(),
    password='',
    decode_responses=True,
    single_connection_client=True,
)


async def get(key: str) -> str:
    return await __instance.get(key)


async def set(
    key: str,
    value: Any,
    ex: Optional[int],
):
    await __instance.set(
        key,
        value,
        ex=ex,
    )


def get_key(*args) -> str:
    return _get_key('', args)
