from typing import Any, Dict, Optional
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.sql_query_executor import SqlQueryExecutor, SqlNonQueryExecutor
from toolbox.sql.connections.connection import Connection, Transaction
from toolbox.sql.connections.sql_server_connection import SqlServerConnection


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
            sql_query.get_query(
                extender='\r\nFOR JSON AUTO, INCLUDE_NULL_VALUES'
            )
        )
        res_json = ''.join(row[0] for row in res_raw.fetchall())
        return res_json
