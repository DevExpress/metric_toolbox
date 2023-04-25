from toolbox.sql_async.protocols import AsyncSqlQueryExecutor, AsyncSqlQuery
from toolbox.sql_async.repository_queries.repository_queries import RepositoryQueries


class AsyncRepository:
    """
    Loads data from the data base.
    The returned data is the result of an sql query.
    """

    def __init__(
        self,
        queries: RepositoryQueries[AsyncSqlQuery],
        query_executor: AsyncSqlQueryExecutor,
    ) -> None:
        self.queries = queries
        self.query_executor = query_executor

    async def get_data(self, **kwargs) -> str:
        return await self.query_executor.execute(
            prep_queries=self.queries.get_prep_queries(kwargs),
            main_query=self.queries.get_main_query(kwargs),
            main_queries=self.queries.get_main_queries(kwargs),
        )

    async def update_data(self, **kwargs) -> None:
        return await self.query_executor.execute_nonquery(
            *self.queries(kwargs)
        )

    async def validate_data(self, **kwargs) -> str:
        return await self.get_data(**kwargs)
