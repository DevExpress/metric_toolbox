from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, Union

from pandas import DataFrame, read_sql
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.base.connection_object import ConnectionObject
from toolbox.sql.query_executors.base.protocols import Transaction


class SqlQueryExecutorBase(ABC):
    """
    Executes and sql query passed to the execute method.
    """

    @abstractmethod
    def execute(
        self,
        sql_query: SqlQuery,
        kwargs: Dict[str, Any],
    ) -> Union[DataFrame, str, None]:
        pass

    def execute_many(
        self,
        prep_queries: Iterable[SqlQuery],
        main_query: SqlQuery,
        main_query_read_kwargs: Dict[str, Any] = {},
    ) -> Union[DataFrame, str]:
        pass

    def execute_many_main_queries(
        self,
        prep_queries: Iterable[SqlQuery],
        main_queries: Dict[str, SqlQuery],
        main_query_read_kwargs: Dict[str, Any] = {},
    ) -> Dict[str, Union[DataFrame, str]]:
        pass

    def _execute_sql_query(
        self,
        sql_query: SqlQuery,
        connection: Any,
        kwargs: Dict[str, Any],
    ) -> Union[DataFrame, str]:
        return read_sql(
            sql=sql_query.get_query(),
            con=connection,
            **kwargs,
        )

    def get_connection_object(self) -> ConnectionObject:
        pass

    def _begin_transaction(self):
        return self.get_connection_object().begin_transaction()

    def _execute_query_func(self, func: Callable[[Transaction], Any]):
        with self._begin_transaction() as transaction:
            res = func(transaction)
        return res
