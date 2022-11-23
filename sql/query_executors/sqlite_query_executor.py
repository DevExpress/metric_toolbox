from typing import Any, Dict

from pandas import DataFrame
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.base.query_executor import SqlQueryExecutorBase
from toolbox.sql.query_executors.base.connection_object import ConnectionObject
from toolbox.sql.query_executors.base.protocols import DbEngine
from toolbox.sql.sqlite_db import get_or_create_db


class SqliteConnectionObject(ConnectionObject):

    def _get_or_create_engine(self) -> DbEngine:
        return get_or_create_db()


class SQLiteQueryExecutor(SqlQueryExecutorBase):

    def get_connection_object(self) -> ConnectionObject:
        return SqliteConnectionObject()

    def execute(
        self,
        sql_query: SqlQuery,
        kwargs: Dict[str, Any] = None,
    ) -> DataFrame:
        with self.get_connection_object().begin_transaction() as transaction:
            query_result = self._execute_sql_query(
                sql_query=sql_query,
                connection=transaction,
                kwargs=kwargs,
            )
        return query_result
