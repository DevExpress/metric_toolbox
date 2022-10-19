from typing import Iterable, Union, Optional
from pandas import DataFrame

from toolbox.cache.redis_cache import RedisCache
from toolbox.utils.converters import (
    DF_to_JSON,
    JSON_to_DF,
    Objects_to_JSON,
)


class CacheObject:

    @staticmethod
    def get_underlying_cache():
        if not hasattr(CacheObject, '_underlying_cache'):
            CacheObject._underlying_cache = RedisCache()
        return CacheObject._underlying_cache

    def __init__(
        self,
        base_key: str,
        expire: int = 300000,

    ):
        self.__base_key = base_key
        self.expire = expire

    def get(self, *args) -> Union[str, None]:
        return CacheObject.get_underlying_cache().get(
            key=self.__get_key(*args)
        )

    def get_df(self, *args, **kwargs) -> DataFrame:
        return JSON_to_DF.convert(json=self.get(*args), **kwargs)

    def __get_key(self, *args):
        return CacheObject.get_underlying_cache().get_key(
            self.__base_key, *args
        )

    def save(self, value: str, key: Optional[Iterable]):
        CacheObject.get_underlying_cache().set(
            key=self.__get_key(*key),
            value=value,
            ex=self.expire,
        )

    def convert_value_to_json_and_save(
        self,
        value: Union[DataFrame, Iterable],
        key: Iterable = [],
    ) -> str:
        if isinstance(value, DataFrame):
            res_json = DF_to_JSON.convert(df=value)
        else:
            res_json = Objects_to_JSON.convert(objects=value)
        self.save(
            key=key,
            value=res_json,
        )
        return res_json
