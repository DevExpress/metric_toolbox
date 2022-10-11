from typing import Iterable, Type, Union
from pandas import DataFrame

import toolbox.sql.columns_validator as columns_validator
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors import SqlQueryExecutor, MSSqlReadQueryExecutor, JsonMSSqlReadQueryExecutor


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
        self.query_executor = query_executor or MSSqlReadQueryExecutor()

    def execute_query(self, **kwargs) -> Union[DataFrame, str, None]:
        query = self.sql_query_type(
            query_file_path=kwargs['query_file_path'],
            format_params=kwargs['query_format_params'],
        )
        return self.query_executor.execute(sql_query=query)

    def get_data(self, **kwargs) -> Union[DataFrame, str]:
        query_result: DataFrame = self.execute_query(**kwargs)
        self.validate_query_result(
            query_result=query_result,
            must_have_columns=kwargs['must_have_columns'],
        )
        return query_result.reset_index(drop=True)

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


class JSONBasedRepository(BaseRepository):

    def __init__(
        self,
        sql_query_type: Type[SqlQuery] = SqlQuery,
        query_executor: SqlQueryExecutor = JsonMSSqlReadQueryExecutor(),
    ) -> None:
        BaseRepository.__init__(
            self,
            sql_query_type=sql_query_type,
            query_executor=query_executor,
        )

    def get_data(self, **kwargs) -> str:
        return self.execute_query(**kwargs)
