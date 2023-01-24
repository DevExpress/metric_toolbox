import os
from typing import Any, Dict, Optional
from sqlalchemy import create_engine
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.sql_query_executor import SqlQueryExecutor, SqlNonQueryExecutor
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
            + '?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
        )


class SqlServerConnection(Connection):

    def _get_or_create_engine(self) -> DbEngine:
        return _get_or_create_engine()


class SqlServerQueryExecutor(SqlQueryExecutor):

    def get_connection_object(self) -> Connection:
        return SqlServerConnection()


class SqlServerNonQueryExecutor(SqlNonQueryExecutor):

    def get_connection_object(self) -> Connection:
        return SqlServerConnection()


class JsonSqlServerReadQueryExecutor(SqlServerQueryExecutor):

    def _execute_query(
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
