import os
import asyncio
import aiosqlite
from collections.abc import Mapping, Iterable, Coroutine
from typing import Optional
from wrapt import decorator
from toolbox.sql_async.protocols import Connection, Connectable, AsyncSqlQuery
import toolbox.logger as Logger


@decorator
async def with_transaction(
    execute_query_func: Coroutine,
    instance: Connectable,
    args: Iterable,
    kwargs: Mapping,
):
    if 'conn' in kwargs:
        return await execute_query_func(*args, **kwargs)
    async with instance.get_connection_object() as conn:
        return await execute_query_func(*args, **kwargs, conn=conn)


class DbConnectable:

    def __init__(self, conn: aiosqlite.Connection = None) -> None:
        self.__conn = conn

    def get_connection_object(self) -> aiosqlite.Connection:
        return self.__conn or aiosqlite.connect(
            database=os.environ['SQLITE_DATABASE']
        )


class AsyncSQLiteQueryExecutor(DbConnectable):

    @with_transaction
    async def execute(
        self,
        *,
        prep_queries: Optional[Iterable[AsyncSqlQuery]] = tuple(),
        main_query: Optional[AsyncSqlQuery] = None,
        main_queries: Optional[Mapping[str, AsyncSqlQuery]] = None,
        conn: Optional[Connection] = None,
    ) -> str:
        await self.execute_nonquery(
            *prep_queries,
            conn=conn,
        )

        if main_query:
            return await self._execute_query(
                query=main_query,
                conn=conn,
            )

        raise NotImplementedError('main_queries')

    @with_transaction
    async def execute_nonquery(
        self,
        *queries: Iterable[AsyncSqlQuery],
        conn: Optional[Connection] = None,
    ) -> None:
        if queries:
            await asyncio.wait(
                [
                    asyncio.create_task(conn.executescript(await script))
                    for script in asyncio.as_completed(queries)
                ]
            )

    async def _execute_query(
        self,
        query: AsyncSqlQuery,
        conn: Connection,
    ) -> str:
        q = await query
        #Logger.debug(q)
        cursor: aiosqlite.Cursor = await conn.execute(q)
        res = await cursor.fetchone()
        return res[0] if res else None
