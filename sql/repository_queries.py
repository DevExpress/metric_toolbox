from collections.abc import Iterable, Mapping
from typing import Type
from toolbox.sql.meta_data import KnotMeta
from toolbox.sql.sql_query import SqlQuery, SqlAlchemyQuery


class RepositoryQueries:
    """Queries descriptor storing query meta data and params."""

    def __init__(
        self,
        main_query_path: str = None,
        main_query_format_params: Mapping[str, str] = KnotMeta.get_attrs(),
        must_have_columns: Iterable[str] = [],
        main_queries: Mapping[str, SqlQuery] = {},
        prep_queries: Iterable[SqlQuery] = tuple(),
        sql_query_type: Type[SqlQuery] = SqlQuery,
    ) -> None:
        self.main_query_path = main_query_path
        self.main_query_format_params = main_query_format_params
        self.must_have_columns = must_have_columns
        self.main_queries = main_queries
        self.prep_queries = prep_queries
        self.sql_query_type = sql_query_type

    def get_main_query_path(self, **kwargs) -> str:
        return kwargs.get('query_file_path', self.main_query_path)

    def get_main_query_format_params(self, **kwargs) -> Mapping[str, str]:
        return kwargs.get('query_format_params', self.main_query_format_params)

    def get_must_have_columns(self, **kwargs) -> Iterable[str]:
        return kwargs.get('must_have_columns', self.must_have_columns)

    def get_main_query(self, **kwargs) -> SqlQuery:
        if not self.get_main_query_path(**kwargs):
            return None
        return self.sql_query_type(
            query_file_path=self.get_main_query_path(**kwargs),
            format_params=self.get_main_query_format_params(**kwargs),
        )

    def get_main_queries(self, **kwargs) -> Mapping[str, SqlQuery]:
        return self.main_queries

    def get_prep_queries(self, **kwargs) -> Iterable[SqlQuery]:
        return self.prep_queries

    def __call__(self, **kwargs) -> Iterable[SqlQuery]:
        return (
            *self.get_prep_queries(**kwargs),
            self.get_main_query(**kwargs),
            *self.get_main_queries(**kwargs).values(),
        )


class RepositoryAlchemyQueries(RepositoryQueries):

    def __init__(self, **kwargs) -> None:
        RepositoryQueries.__init__(self, sql_query_type=SqlAlchemyQuery, **kwargs)
