import redis
import toolbox.config as config
from typing import Any, Optional
from toolbox.cache.utils import get_key as _get_key


__instance = None


def __ensure_instance():
    global __instance
    if __instance is None:
        __instance = redis.Redis(
            host=config.redis_service(),
            port=config.redis_port(),
            password='',
            decode_responses=True,
        )


def get(key: str) -> str:
    __ensure_instance()
    return __instance.get(key)


def set(
    key: str,
    value: Any,
    ex: Optional[int],
):
    __ensure_instance()
    __instance.set(
        key,
        value,
        ex=ex,
    )


def get_key(*args) -> str:
    return _get_key('', args)
