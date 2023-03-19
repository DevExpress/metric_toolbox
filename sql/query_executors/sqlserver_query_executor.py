from functools import partial
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.sql_query_executor import (
    SqlQueryExecutor,
    SqlNonQueryExecutor,
)
from toolbox.sql.connections.connection import Transaction
from toolbox.sql.connections.sql_server_connection import SqlServerConnection


SqlServerQueryExecutor = partial(SqlQueryExecutor, conn=SqlServerConnection())
SqlServerNonQueryExecutor = partial(SqlNonQueryExecutor, conn=SqlServerConnection())

class SqlServerJsonQueryExecutor(SqlQueryExecutor):

    def __init__(self) -> None:
        super().__init__(conn=SqlServerConnection())

    def _execute_query(
        self,
        query: SqlQuery,
        tran: Transaction,
        **kwargs,
    ) -> str:
        res_raw = tran.execute(
            query.get_script(
                extender='\r\nFOR JSON AUTO, INCLUDE_NULL_VALUES'
            )
        )
        res_json = ''.join(row[0] for row in res_raw.fetchall())
        return res_json
