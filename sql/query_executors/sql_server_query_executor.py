import os
from typing import Any, Dict, Iterable, Optional, Union
from pandas import DataFrame
from sqlalchemy import create_engine
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.sql_query_executor import SqlQueryExecutor
from toolbox.sql.query_executors.connection import (
    DbEngine,
    Transaction,
    Connection,
)


_engine = None


def _get_or_create_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(ConnectionParams().get_url())
    return _engine


class ConnectionParams:

    def __init__(
        self,
        user_env: str = 'SQL_USER',
        password_env: str = 'SQL_PASSWORD',
        server_env: str = 'SQL_SERVER',
        data_base_env: str = 'SQL_DATABASE',
    ):
        self.user = os.environ[user_env]
        self.password = os.environ[password_env]
        self.server = os.environ[server_env]
        self.data_base = os.environ[data_base_env]

    def get_url(self):
        return (
            'mssql+pyodbc://' + self.user + ':' + self.password + '@'
            + self.server + '/' + self.data_base
            + '?driver=ODBC Driver 17 for SQL Server'
        )


class SqlServerConnection(Connection):

    def _get_or_create_engine(self) -> DbEngine:
        return _get_or_create_engine()


class SqlServerQueryExecutor(SqlQueryExecutor):

    def get_connection_object(self) -> Connection:
        return SqlServerConnection()


class SqlServerPostQueryExecutor(SqlServerQueryExecutor):

    def execute(
        self,
        sql_query: SqlQuery,
        kwargs: Dict[str, Any] = None,
    ) -> str:

        def func(conn: Transaction):
            self._execute_prep_queries(
                prep_queries=[sql_query],
                conn=conn,
            )

        self._execute_query_func(func=func)

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


class JsonSqlServerReadQueryExecutor(SqlServerQueryExecutor):

    def _execute_sql_query(
        self,
        sql_query: SqlQuery,
        connection: Transaction,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> str:
        res_raw = connection.execute(
            sql_query.get_query() + '\r\nFOR JSON AUTO, INCLUDE_NULL_VALUES'
        )
        res_json = ''.join(row[0] for row in res_raw.fetchall())
        return res_json
