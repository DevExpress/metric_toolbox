import redis
import os
from typing import Any, Optional


class RedisCache:

    def __init__(self, db_name: str = None) -> None:
        self.db_name = db_name or os.environ['REDIS_DB']
        self.r: redis.Redis = None

    def connect(self):
        if self.r != None:
            self.r.close()
        self.r = redis.Redis(
            host=os.environ['REDIS_SERVICE'],
            port=os.environ['REDIS_PORT'],
            password='',
            decode_responses=True,
        )

    def set(
        self,
        key: str,
        value: Any,
        ex: Optional[int],
    ):
        if self.r is None:
            self.connect()
        self.r.set(
            key,
            value,
            ex=ex,
        )

    def cache(
        self,
        key: str,
        val: str,
        ex: Optional[int] = None,
    ) -> None:
        self.set(
            key=key,
            value=val,
            ex=ex,
        )

    def get(self, key: str) -> str:
        if self.r is None:
            self.connect()
        return self.r.get(key)

    def _get_key(self, *kargs) -> str:
        return f'{self.db_name}:{":".join(str(arg) for arg in kargs)}'
