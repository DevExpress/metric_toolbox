from collections.abc import Iterable, Mapping
from typing import Generic, TypeVar
from toolbox.sql.meta_data import MetaData


T = TypeVar('T')


class QueryDescriptor(Generic[T]):

    def __init__(
        self,
        path: str = None,
        fields_meta: MetaData = None,
        format_params: Mapping[str, str] = {},
        query_type: T = T,
        **kwargs,
    ) -> None:
        self.path = path
        self.format_params = format_params
        self.fields_meta = fields_meta
        self.query_type = query_type
        self.kwargs = kwargs

    def get_path(self, kwargs: Mapping) -> str:
        return kwargs.get('path', self.path)

    def get_fields_meta(self, kwargs: Mapping) -> MetaData:
        return kwargs.get('fields_meta', self.format_params)

    def get_fields(self, kwargs: Mapping) -> Iterable[str]:
        return self.get_fields_meta(kwargs).get_values()

    def get_format_params(self, kwargs: Mapping) -> Mapping[str, str]:
        return kwargs.get('format_params', self.format_params)

    def get_query(self, kwargs: Mapping) -> T:
        return self.query_type(
            query_file_path=self.get_path(kwargs),
            fields_mapping=self.get_fields_meta(kwargs).get_attrs(),
            fields=self.get_fields(kwargs),
            format_params=self.get_format_params(kwargs),
            **self.kwargs,
        )
