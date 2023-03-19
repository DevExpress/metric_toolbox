from __future__ import annotations
from wrapt import decorator
from collections.abc import Mapping, Iterable
from typing import Union, Optional
from pandas import DataFrame, read_sql
import toolbox.logger as Logger
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.connections.connection import (
    Transaction,
    DbConnectable,
    Connectable,
)


@decorator
def with_transaction(execute_query_func, instance: Connectable, args, kwargs):
    if 'tran' in kwargs:
        return execute_query_func(*args, **kwargs)
    tran = instance.get_connection_object()
    with tran.begin_transaction() as transaction:
        return execute_query_func(*args, **kwargs, tran=transaction)


class SqlQueryExecutor(DbConnectable):

    @with_transaction
    def execute(
        self,
        *,
        prep_queries: Optional[Iterable[SqlQuery]] = None,
        main_query: Optional[SqlQuery] = None,
        main_queries: Optional[Mapping[str, SqlQuery]] = None,
        tran: Optional[Transaction] = None,
    ) -> DataFrame:
        self.execute_nonquery(*prep_queries, tran=tran)

        if main_query:
            Logger.debug(main_query._file_path)
            return self._execute_query(
                query=main_query,
                tran=tran,
            )

        res = {}
        for k, query in main_queries.items():
            Logger.debug(f'{k} : {query._file_path}')
            res[k] = self._execute_query(
                query=query,
                tran=tran,
            )
        return res

    @with_transaction
    def execute_nonquery(
        self,
        *queries: Union[SqlQuery, str],
        tran: Optional[Transaction] = None,
    ) -> None:
        for query in queries:
            if isinstance(query, SqlQuery):
                Logger.debug(query._file_path)
                query = query.get_script()

            self._execute_script(script=query, tran=tran)

    def _execute_query(
        self,
        query: SqlQuery,
        tran: Transaction,
        **kwargs,
    ) -> Union[DataFrame, str]:
        return read_sql(
            sql=query.get_script(),
            con=tran,
            **kwargs,
        )

    def _execute_script(
        self,
        script: str,
        tran: Transaction,
    ):
        execute = tran.execute
        if hasattr(tran, 'executescript'):
            execute = tran.executescript
        execute(script)


class SqlNonQueryExecutor(SqlQueryExecutor):

    @with_transaction
    def execute(
        self,
        *,
        prep_queries: Optional[Iterable[SqlQuery]] = None,
        main_query: Optional[SqlQuery] = None,
        main_queries: Optional[Mapping[str, SqlQuery]] = None,
        tran: Optional[Transaction] = None,
    ) -> DataFrame:
        self.execute_nonquery(*prep_queries, tran=tran)
