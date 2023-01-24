from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, Union, Optional
from pandas import DataFrame, read_sql
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.connection import (
    Transaction,
    Connection,
)
from toolbox.logger import Logger


class SqlQueryExecutor(ABC):

    def execute_non_query(
        self,
        sql_query: SqlQuery,
    ) -> None:

        def func(conn: Transaction):
            self._execute_non_query(
                sql_query=sql_query,
                conn=conn,
            )

        self._execute_query_func(func=func)

    def execute_many(
        self,
        prep_queries: Iterable[SqlQuery],
        main_query: SqlQuery,
        main_query_read_kwargs: Dict[str, Any] = {},
    ) -> DataFrame:

        def func(conn: Transaction):
            self._execute_prep_queries(prep_queries, conn)
            Logger.debug(main_query._query_file_path)
            return self._execute_query(
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
                res[k] = self._execute_query(
                    sql_query=v,
                    connection=conn,
                    kwargs=main_query_read_kwargs,
                )
            return res

        return self._execute_query_func(func=func)

    def _execute_query(
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

    def _execute_non_query(
        self,
        sql_query: SqlQuery,
        conn: Transaction,
    ):
        execute = conn.execute
        if hasattr(conn, 'executescript'):
            execute = conn.executescript
        execute(sql_query.get_query())

    def _execute_prep_queries(
        self,
        prep_queries: Iterable[SqlQuery],
        conn: Transaction,
    ):
        for sql_query in prep_queries:
            Logger.debug(sql_query._query_file_path)
            self._execute_non_query(
                sql_query=sql_query,
                conn=conn,
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


class SqlNonQueryExecutor(SqlQueryExecutor):

    def execute_many(
        self,
        prep_queries: Iterable[SqlQuery],
        main_query: SqlQuery = None,
        main_query_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:

        def func(conn: Transaction):
            self._execute_prep_queries(
                prep_queries=prep_queries,
                conn=conn,
            )

        self._execute_query_func(func=func)

    def execute_many_main_queries(
        self,
        prep_queries: Iterable[SqlQuery],
        main_queries: Dict[str, SqlQuery],
        main_query_read_kwargs: Dict[str, Any] = {},
    ) -> Dict[str, Union[DataFrame, str]]:
        raise NotImplementedError()
