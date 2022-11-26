from toolbox.sql.query_executors.sql_query_executor import SqlQueryExecutor
from toolbox.sql.query_executors.connection import (
    DbEngine,
    Connection,
)

from toolbox.sql.sqlite_db import get_or_create_db


class SqliteConnection(Connection):

    def _get_or_create_engine(self) -> DbEngine:
        return get_or_create_db()


class SQLiteQueryExecutor(SqlQueryExecutor):

    def get_connection_object(self) -> Connection:
        return SqliteConnection()
