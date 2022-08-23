import redis
import os
from typing import Any, Optional


class RedisCache:

    def __init__(self) -> None:
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
        ex: Optional[int] = None,
    ):
        if self.r is None:
            self.connect()
        self.r.set(
            key,
            value,
            ex=ex,
        )

    def cache(self, db, key: str, val: str) -> None:
        key = f'{os.environ[db]}:{key}'
        self.set(key, val)

    def get(self, key: str) -> str:
        if self.r is None:
            self.connect()
        return self.r.get(key)
