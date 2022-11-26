from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, Union
from pandas import DataFrame, read_sql
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.connection import (
    Transaction,
    Connection,
)
from toolbox.logger import Logger


class SqlQueryExecutor(ABC):
    """
    Executes and sql query passed to the execute method.
    """

    def execute(
        self,
        sql_query: SqlQuery,
        kwargs: Dict[str, Any] = {},
    ) -> DataFrame:

        def func(conn: Transaction):
            return self._execute_sql_query(
                sql_query=sql_query,
                connection=conn,
                kwargs=kwargs,
            )

        return self._execute_query_func(func=func)

    def _execute_prep_queries(
        self,
        prep_queries: Iterable[SqlQuery],
        conn: Transaction,
    ):
        for sql_query in prep_queries:
            Logger.debug(sql_query._query_file_path)
            conn.execute(sql_query.get_query())

    def execute_many(
        self,
        prep_queries: Iterable[SqlQuery],
        main_query: SqlQuery,
        main_query_read_kwargs: Dict[str, Any] = {},
    ) -> DataFrame:

        def func(conn: Transaction):
            self._execute_prep_queries(prep_queries, conn)
            Logger.debug(main_query._query_file_path)
            return self._execute_sql_query(
                sql_query=main_query,
                connection=conn,
                kwargs=main_query_read_kwargs,
            )

        return self._execute_query_func(func=func)

    def execute_many_main_queries(
        self,
        prep_queries: Iterable[SqlQuery],
        main_queries: Dict[str, SqlQuery],
        main_query_read_kwargs: Dict[str, Any] = {},
    ) -> Dict[str, DataFrame]:

        def func(conn: Transaction):
            self._execute_prep_queries(prep_queries, conn)
            res = {}
            for k, v in main_queries.items():
                Logger.debug(f'{k} : {v._query_file_path}')
                res[k] = self._execute_sql_query(
                    sql_query=v,
                    connection=conn,
                    kwargs=main_query_read_kwargs,
                )
            return res

        return self._execute_query_func(func=func)

    def _execute_sql_query(
        self,
        sql_query: SqlQuery,
        connection: Transaction,
        kwargs: Dict[str, Any],
    ) -> Union[DataFrame, str]:
        return read_sql(
            sql=sql_query.get_query(),
            con=connection,
            **kwargs,
        )

    @abstractmethod
    def get_connection_object(self) -> Connection:
        pass

    def _begin_transaction(self):
        return self.get_connection_object().begin_transaction()

    def _execute_query_func(self, func: Callable[[Transaction], Any]):
        with self._begin_transaction() as transaction:
            res = func(transaction)
        return res
