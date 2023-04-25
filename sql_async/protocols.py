from collections.abc import Mapping, Iterable
from typing import Optional, Protocol
from contextlib import AbstractAsyncContextManager


class Connection(AbstractAsyncContextManager, Protocol):

    async def execute(self, *kargs, **kwargs):
        pass

    async def executescript(self, *kargs, **kwargs):
        pass


class Connectable(Protocol):

    def get_connection_object(self) -> Connection:
        ...


class AsyncSqlQuery(Protocol):

    def __await__(self):
        pass


class AsyncSqlQueryExecutor(Protocol):

    async def execute(
        self,
        *,
        prep_queries: Optional[Iterable[AsyncSqlQuery]],
        main_query: Optional[AsyncSqlQuery],
        main_queries: Optional[Mapping[str, AsyncSqlQuery]],
        conn: Optional[Connection],
    ):
        pass

    async def execute_nonquery(
        self,
        *queries: Iterable[AsyncSqlQuery],
        conn: Optional[Connection],
    ) -> None:
        pass


class AsyncRepository:

    async def get_data(self, **kwargs):
        pass

    async def update_data(self, **kwargs) -> None:
        pass

    async def validate_data(self, **kwargs):
        pass
