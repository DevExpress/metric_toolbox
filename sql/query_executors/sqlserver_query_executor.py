from typing import Any, Dict, Optional
from functools import partial
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.sql_query_executor import (
    SqlQueryExecutor,
    SqlNonQueryExecutor,
)
from toolbox.sql.connections.connection import Transaction
from toolbox.sql.connections.sql_server_connection import SqlServerConnection


SqlServerQueryExecutor = partial(SqlQueryExecutor, conn=SqlServerConnection())
SqlServerNonQueryExecutor = partial(
    SqlNonQueryExecutor, conn=SqlServerConnection()
)


class SqlServerJsonQueryExecutor(SqlQueryExecutor):

    def __init__(self) -> None:
        super().__init__(conn=SqlServerConnection())

    def _execute_query(
        self,
        sql_query: SqlQuery,
        conn: Transaction,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> str:
        res_raw = conn.execute(
            sql_query.get_query(
                extender='\r\nFOR JSON AUTO, INCLUDE_NULL_VALUES'
            )
        )
        res_json = ''.join(row[0] for row in res_raw.fetchall())
        return res_json
