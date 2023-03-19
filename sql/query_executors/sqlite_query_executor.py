from functools import partial
from toolbox.sql.connections.sqlite_connection import SqliteConnection
from toolbox.sql.query_executors.sql_query_executor import (
    SqlQueryExecutor,
    SqlNonQueryExecutor,
)


SQLiteQueryExecutor = partial(SqlQueryExecutor, conn=SqliteConnection())
SQLiteNonQueryExecutor = partial(SqlNonQueryExecutor, conn=SqliteConnection())
