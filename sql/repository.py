from typing import Union, Dict
from functools import partial
import toolbox.sql.columns_validator as columns_validator
from toolbox.sql.repository_queries import RepositoryQueries
from pandas import DataFrame
from toolbox.sql.query_executors.sqlserver_query_executor import (
    SqlServerJsonQueryExecutor,
    SqlServerQueryExecutor,
    SqlQueryExecutor,
)
from toolbox.sql.query_executors.sqlite_query_executor import SQLiteQueryExecutor
from toolbox.utils.converters import DF_to_JSON


class Repository:
    """
    Loads data from the data base.
    The returned data is the result of an sql query.
    """

    def __init__(
        self,
        queries: RepositoryQueries = RepositoryQueries(),
        query_executor: SqlQueryExecutor = None,
    ) -> None:
        self.queries = queries
        self.query_executor = query_executor or SqlServerQueryExecutor()

    @classmethod
    def create(cls):
        return cls()

    # yapf: disable
    def get_data(self, **kwargs) -> Union[Dict[str, DataFrame], DataFrame, str]:
        query_result= self.query_executor.execute_many(
            prep_queries=self.queries.get_prep_queries(**kwargs),
            main_query=self.queries.get_main_query(**kwargs),
        )

        if isinstance(query_result, DataFrame):
            columns_validator.ensure_must_have_columns(
                df=query_result,
                must_have_columns=self.queries.get_must_have_columns(**kwargs),
            )
            return query_result.reset_index(drop=True)
        return query_result
    # yapf: enable

    def get_data_json(self, **kwargs) -> str:
        query_result = self.get_data(**kwargs)
        if isinstance(query_result, DataFrame):
            return DF_to_JSON.convert(df = query_result)
        return query_result

    def update_data(self, **kwargs) -> None:
        return self.query_executor.execute_many(
            prep_queries=(
                *self.queries.get_prep_queries(**kwargs),
                self.queries.get_main_query(**kwargs),
            )
        )

    def validate_data(self, **kwargs) -> str:
        return self.get_data_json(**kwargs)


JSONBasedRepository = partial(Repository, query_executor=SqlServerJsonQueryExecutor())
SqlServerRepository = partial(Repository, query_executor=SqlServerQueryExecutor())
SqliteRepository = partial(Repository, query_executor=SQLiteQueryExecutor())
