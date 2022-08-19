import os
from typing import Any, Dict, NamedTuple
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pandas import DataFrame, read_sql
from abc import ABC, abstractmethod

from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.sqlite_data_base import SQLiteDataBase


class ConnectionParams(NamedTuple):
    user: str
    password: str
    server: str
    data_base: str


class SqlQueryExecutor(ABC):
    """
    Executes and sql query passed to the execute method.
    """

    @abstractmethod
    def execute(
        self,
        sql_query: SqlQuery,
        kargs: Dict[str, Any],
    ) -> DataFrame:
        pass

    def _execute_sql_query(
        self,
        sql_query: SqlQuery,
        connection: Any,
        kargs: Dict[str, Any],
    ) -> DataFrame:
        return read_sql(
            sql=sql_query.get_query(),
            con=connection,
            **kargs,
        )


class MSSqlQueryExecutor(SqlQueryExecutor):

    def __init__(
        self,
        user_env: str = 'SQL_USER',
        password_env: str = 'SQL_PASSWORD',
        server_env: str = 'SQL_SERVER',
        data_base_env: str = 'SQL_DATABASE',
    ):
        self.user_env = user_env
        self.password_env = password_env
        self.server_env = server_env
        self.data_base_env = data_base_env

    def _get_connection_params(self) -> ConnectionParams:
        return ConnectionParams(
            user=os.environ[self.user_env],
            password=os.environ[self.password_env],
            server=os.environ[self.server_env],
            data_base=os.environ[self.data_base_env],
        )

    def _create_engine(self) -> Engine:
        params = self._get_connection_params()
        return create_engine(
            'mssql+pyodbc://' + params.user + ':' + params.password + '@'
            + params.server + '/' + params.data_base
            + '?driver=ODBC Driver 17 for SQL Server',
        )

    def execute(
        self,
        sql_query: SqlQuery,
        kargs: Dict[str, Any] = {},
    ) -> DataFrame:
        engine = self._create_engine()
        query_result = self._execute_sql_query(
            sql_query=sql_query,
            connection=engine,
            kargs=kargs,
        )
        engine.dispose()
        return query_result


class SQLiteQueryExecutor(SqlQueryExecutor):

    def __init__(
        self,
        data_base: str = None,
    ):
        self.data_base = SQLiteDataBase(name=data_base)

    def execute(
        self,
        sql_query: SqlQuery,
        source_tables: Dict[str, DataFrame],
        kargs: Dict[str, Any] = {},
    ) -> DataFrame:
        self.data_base.try_connect()
        self.data_base.save_tables(source_tables)

        query_result = self._execute_sql_query(
            sql_query=sql_query,
            connection=self.data_base.get_connection(),
            kargs=kargs,
        )

        self.data_base.try_disconnect()
        return query_result
