from collections.abc import Iterable, Mapping
from typing import Generic, TypeVar
from toolbox.sql_async.repository_queries.query_descriptor import QueryDescriptor


T = TypeVar('T')


class RepositoryQueries(Generic[T]):

    def __init__(
        self,
        main_query: QueryDescriptor[T],
        main_queries: Mapping[str, QueryDescriptor[T]] = {},
        prep_queries: Iterable[QueryDescriptor[T]] = tuple()
    ) -> None:
        self.main_query = main_query
        self.main_queries = main_queries
        self.prep_queries = prep_queries

    def get_main_query(self, kwargs: Mapping) -> T:
        return self.main_query.get_query(kwargs)

    def get_main_queries(self, kwargs: Mapping) -> Mapping[str, T]:
        return {
            query_name: query_descriptor.get_query(kwargs)
            for query_name, query_descriptor in self.main_queries.items()
        }

    def get_prep_queries(self, kwargs: Mapping) -> Iterable[T]:
        return {
            query_descriptor.get_query(kwargs)
            for query_descriptor in self.prep_queries
        }

    def __call__(self, kwargs: Mapping) -> Iterable[T]:
        return (
            *self.get_prep_queries(kwargs),
            self.get_main_query(kwargs),
            *self.get_main_queries(kwargs).values(),
        )
