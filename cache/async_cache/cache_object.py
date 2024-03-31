from typing import Iterable, Optional
import toolbox.cache.async_cache.redis_cache as _redis_cache


class CacheObject:

    def __init__(
        self,
        base_key: str,
        expire: Optional[int] = 300000,
    ):
        self.__base_key = base_key
        self.expire = expire

    async def get(self, *args) -> Optional[str]:
        return await _redis_cache.get(key=self.__get_key(args))

    def __get_key(self, args):
        return _redis_cache.get_key(self.__base_key, *args)

    async def save(self, value: str, key: Iterable = tuple()) -> str:
        await _redis_cache.set(
            key=self.__get_key(key),
            value=value,
            ex=self.expire,
        )
        return value
