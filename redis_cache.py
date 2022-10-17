import redis
import os
from typing import Any, Optional


class RedisCache:

    def __init__(self, db_name: str = None) -> None:
        self.db_name = db_name

    def ensure_connection(self):
        if not hasattr(RedisCache, 'redis_instance'):
            RedisCache.redis_instance = redis.Redis(
                host=os.environ['REDIS_SERVICE'],
                port=os.environ['REDIS_PORT'],
                password='',
                decode_responses=True,
            )

    def get(self, key: str) -> str:
        self.ensure_connection()
        return RedisCache.redis_instance.get(key)

    def set(
        self,
        key: str,
        value: Any,
        ex: Optional[int],
    ):
        self.ensure_connection()
        RedisCache.redis_instance.set(
            key,
            value,
            ex=ex,
        )

    def get_key(self, *kargs) -> str:
        return f'{self._get_db_name()}:{":".join(str(arg) for arg in kargs)}'

    def _get_db_name(self):
        return self.db_name or os.environ['REDIS_DB']
