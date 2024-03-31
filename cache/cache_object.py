from typing import Iterable, Union, Any, Optional
from pandas import DataFrame

import toolbox.cache.redis_cache as _redis_cache
from toolbox.utils.converters import (
    DF_to_JSON,
    JSON_to_DF,
    Objects_to_JSON,
    Object_to_JSON,
    JSON_to_object,
)


class CacheObject:

    def _get_cache(self):
        return _redis_cache

    def __init__(
        self,
        base_key: str,
        expire: Optional[int] = 300000,
    ):
        self.__base_key = base_key
        self.expire = expire

    def get(self, *args) -> Optional[str]:
        return self._get_cache().get(key=self.__get_key(args))

    def get_df(self, *args, **kwargs) -> DataFrame:
        return JSON_to_DF.convert(json=self.get(*args), **kwargs)

    def get_object(self, *args, **kwargs) -> Any:
        return JSON_to_object.convert(json_obj=self.get(*args), **kwargs)

    def __get_key(self, args: Iterable):
        return self._get_cache().get_key(self.__base_key, *args)

    def save(self, value: str, key: Iterable = ()) -> str:
        self._get_cache().set(
            key=self.__get_key(key),
            value=value,
            ex=self.expire,
        )
        return value

    def convert_value_to_json_and_save(
        self,
        value: Union[DataFrame, Iterable],
        key: Iterable = (),
        deep_convert: bool = True,
    ) -> str:
        if isinstance(value, DataFrame):
            res_json = DF_to_JSON.convert(df=value)
        elif deep_convert:
            res_json = Objects_to_JSON.convert(objects=value)
        else:
            res_json = Object_to_JSON.convert(obj=value)
        self.save(
            key=key,
            value=res_json,
        )
        return res_json
