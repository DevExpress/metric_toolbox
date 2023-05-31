from collections.abc import Mapping
from toolbox.sql.meta_data import MetaData
from toolbox.sql_async.repository_queries.query_descriptor import QueryDescriptor
from toolbox.sql_async.sql_query import AsyncSqlQuery, GeneralSelectAsyncSqlQuery


class AsyncQueryDescriptor(QueryDescriptor):

    def __init__(
        self,
        path: str = None,
        fields_meta: MetaData = None,
        format_params: Mapping[str, str] = {},
        **kwargs,
    ) -> None:
        super().__init__(
            path=path,
            fields_meta=fields_meta,
            format_params=format_params,
            query_type=AsyncSqlQuery,
            **kwargs,
        )


class GeneralSelectAsyncQueryDescriptor(QueryDescriptor):

    def __init__(
        self,
        fields_meta: MetaData = None,
        format_params: Mapping[str, str] = {},
        **kwargs,
    ) -> None:
        super().__init__(
            fields_meta=fields_meta,
            format_params=format_params,
            query_type=GeneralSelectAsyncSqlQuery,
            **kwargs,
        )
