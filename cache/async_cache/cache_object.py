from typing import Iterable, Optional

from toolbox.cache.async_cache.redis_cache import RedisCache


_cache = None


def _get_or_create_cache():
    global _cache
    if _cache is None:
        _cache = RedisCache()
    return _cache


class CacheObject:

    def _get_cache(self):
        return _get_or_create_cache()

    def __init__(
        self,
        base_key: str,
        expire: Optional[int] = 300000,
    ):
        self.__base_key = base_key
        self.expire = expire

    async def get(self, *args) -> Optional[str]:
        return await self._get_cache().get(key=self.__get_key(*args))

    def __get_key(self, *args):
        return self._get_cache().get_key(self.__base_key, *args)

    async def save(self, value: str, key: Iterable = ()) -> str:
        await self._get_cache().set(
            key=self.__get_key(*key),
            value=value,
            ex=self.expire,
        )
        return value
