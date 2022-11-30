from typing import Iterable, Type, Union, Dict, List
import toolbox.sql.columns_validator as columns_validator
from pandas import DataFrame
from toolbox.sql.query_executors.sql_server_query_executor import (
    JsonSqlServerReadQueryExecutor,
    SqlServerQueryExecutor,
    SqlQueryExecutor,
)
from toolbox.sql.query_executors.sqlite_query_executor import SQLiteQueryExecutor
from toolbox.sql.sql_query import SqlQuery
from toolbox.utils.converters import DF_to_JSON


class Repository:
    """
    Loads data from the data base.
    The returned data is the result of an sql query.
    """

    def __init__(
        self,
        sql_query_type: Type[SqlQuery] = SqlQuery,
        query_executor: SqlQueryExecutor = None,
    ) -> None:
        self.sql_query_type = sql_query_type
        self.query_executor = query_executor or SqlServerQueryExecutor()

    @classmethod
    def create(cls):
        return cls()

    def execute_query(self, **kwargs) -> Union[DataFrame, str, None]:
        query = self.sql_query_type(
            query_file_path=self.get_main_query_path(kwargs),
            format_params=self.get_main_query_format_params(kwargs),
        )
        return self.query_executor.execute(sql_query=query)

    # yapf: disable
    def get_data(self, **kwargs) -> Union[Dict[str, DataFrame], DataFrame, str]:
        query_result: DataFrame = self.execute_query(**kwargs)
        self.validate_query_result(
            query_result=query_result,
            must_have_columns=self.get_must_have_columns(kwargs),
        )
        return query_result.reset_index(drop=True)
    # yapf: enable

    def get_data_json(self, **kwargs) -> str:
        return DF_to_JSON.convert(self.get_data(**kwargs))

    def update_data(self, **kwargs) -> None:
        return self.execute_query(**kwargs)

    def validate_query_result(
        self,
        query_result: DataFrame,
        must_have_columns: Iterable[str],
    ):
        columns_validator.ensure_must_have_columns(
            df=query_result,
            must_have_columns=must_have_columns,
        )

    def get_main_query_path(self, kwargs: Dict) -> str:
        return kwargs['query_file_path']

    def get_main_query_format_params(self, kwargs: Dict) -> Dict[str, str]:
        return kwargs['query_format_params']

    def get_must_have_columns(self, kwargs: Dict) -> List[str]:
        return kwargs['must_have_columns']


class JSONBasedRepository(Repository):

    def __init__(
        self,
        sql_query_type: Type[SqlQuery] = SqlQuery,
        query_executor: SqlQueryExecutor = JsonSqlServerReadQueryExecutor(),
    ) -> None:
        Repository.__init__(
            self,
            sql_query_type=sql_query_type,
            query_executor=query_executor,
        )

    def get_data(self, **kwargs) -> str:
        return self.execute_query(**kwargs)

    def get_data_json(self, **kwargs) -> str:
        return self.get_data(**kwargs)


class SqliteRepository(Repository):

    def __init__(
        self,
        sql_query_type: Type[SqlQuery] = SqlQuery,
        query_executor: SqlQueryExecutor = SQLiteQueryExecutor(),
    ) -> None:
        Repository.__init__(
            self,
            sql_query_type=sql_query_type,
            query_executor=query_executor,
        )
