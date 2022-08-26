from typing import Iterable, Type
from pandas import DataFrame

from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors import SqlQueryExecutor, MSSqlQueryExecutor
import toolbox.sql.columns_validator as columns_validator


class BaseRepository:
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
        self.query_executor = query_executor or MSSqlQueryExecutor()

    def get_data(self, **kargs) -> DataFrame:
        query = self.sql_query_type(
            query_file_path=kargs['query_file_path'],
            format_params=kargs['query_format_params'],
        )
        query_result: DataFrame = self.query_executor.execute(sql_query=query)
        self.validate_query_result(
            query_result=query_result,
            must_have_columns=kargs['must_have_columns'],
        )
        return query_result.reset_index(drop=True)

    def validate_query_result(
        self,
        query_result: DataFrame,
        must_have_columns: Iterable[str],
    ):
        columns_validator.ensure_must_have_columns(
            df=query_result,
            must_have_columns=must_have_columns,
        )
