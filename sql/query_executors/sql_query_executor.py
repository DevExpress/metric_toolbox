from __future__ import annotations
from typing import Any, Dict, Iterable, Union, Optional
from wrapt import decorator
from pandas import DataFrame, read_sql
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.connections.connection import (
    Transaction,
    Connection,
)
from toolbox.logger import Logger


@decorator
def with_transaction(execute_query_func, instance: SqlQueryExecutor, args, kwargs):
    if 'conn' in kwargs:
        return execute_query_func(*args, **kwargs)
    conn = instance.get_connection_object()
    with conn.begin_transaction() as transaction:
        return execute_query_func(*args, **kwargs, conn=transaction)


class SqlQueryExecutor:

    def __init__(self, conn: Connection) -> None:
        self.__conn = conn

    @with_transaction
    def execute_script_non_query(
        self,
        script: str,
        conn: Optional[Transaction] = None,
    ) -> None:
        self._execute_script(script=script, conn=conn)

    @with_transaction
    def execute_non_query(
        self,
        sql_query: SqlQuery,
        conn: Optional[Transaction] = None,
    ) -> None:
        self._execute_non_query(sql_query=sql_query, conn=conn)

    @with_transaction
    def execute_many(
        self,
        prep_queries: Iterable[SqlQuery],
        main_query: SqlQuery,
        main_query_read_kwargs: Dict[str, Any] = {},
        conn: Optional[Transaction] = None,
    ) -> DataFrame:
        self._execute_prep_queries(prep_queries=prep_queries, conn=conn)
        Logger.debug(main_query._query_file_path)
        return self._execute_query(
            sql_query=main_query,
            kwargs=main_query_read_kwargs,
            conn=conn,
        )

    @with_transaction
    def execute_many_main_queries(
        self,
        prep_queries: Iterable[SqlQuery],
        main_queries: Dict[str, SqlQuery],
        main_query_read_kwargs: Dict[str, Any] = {},
        conn: Optional[Transaction] = None,
    ) -> Dict[str, DataFrame]:
        self._execute_prep_queries(prep_queries=prep_queries, conn=conn)
        res = {}
        for k, v in main_queries.items():
            Logger.debug(f'{k} : {v._query_file_path}')
            res[k] = self._execute_query(
                sql_query=v,
                kwargs=main_query_read_kwargs,
                conn=conn,
            )
        return res

    def _execute_query(
        self,
        sql_query: SqlQuery,
        conn: Transaction,
        kwargs: Dict[str, Any],
    ) -> Union[DataFrame, str]:
        return read_sql(
            sql=sql_query.get_query(),
            con=conn,
            **kwargs,
        )

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

    def _execute_non_query(
        self,
        sql_query: SqlQuery,
        conn: Transaction,
    ):
        self._execute_script(
            script=sql_query.get_query(),
            conn=conn,
        )

    def _execute_script(
        self,
        script: str,
        conn: Transaction,
    ):
        execute = conn.execute
        if hasattr(conn, 'executescript'):
            execute = conn.executescript
        execute(script)

    def get_connection_object(self) -> Connection:
        return self.__conn


class SqlNonQueryExecutor(SqlQueryExecutor):

    @with_transaction
    def execute_many(
        self,
        prep_queries: Iterable[SqlQuery],
        main_query: Optional[SqlQuery] = None,
        main_query_kwargs: Optional[Dict[str, Any]] = None,
        conn: Optional[Transaction] = None,
    ) -> None:
        self._execute_prep_queries(prep_queries=prep_queries, conn=conn)

    def execute_many_main_queries(
        self,
        prep_queries: Iterable[SqlQuery],
        main_queries: Dict[str, SqlQuery],
        main_query_read_kwargs: Dict[str, Any] = {},
        conn: Optional[Transaction] = None,
    ) -> Dict[str, Union[DataFrame, str]]:
        raise NotImplementedError()
