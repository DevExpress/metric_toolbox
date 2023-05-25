from collections.abc import Mapping
from typing import NamedTuple
from pandas import DataFrame
from toolbox.sql.crud_queries.protocols import CRUDQuery
from toolbox.sql.connections.connection import (
    Transaction,
    with_transaction,
    DbConnectable,
    Connection,
)


class SaveTableOperation(DbConnectable):

    def __init__(
        self,
        conn: Connection,
        query: CRUDQuery,
        tables_defs: Mapping[str, str] = {},
        create_index_statements: Mapping[str, str] = {},
    ) -> None:
        super().__init__(conn)
        self._query = query
        self._tables_defs = tables_defs
        self._create_index_statements = create_index_statements

    @with_transaction
    def __call__(self, tran: Transaction):
        table_created_manually = self.__try_create_table(tran)

        if table_created_manually:
            self.__try_create_index(tran)

        self._save_table(tran, table_created_manually)

        if not table_created_manually:
            self.__try_create_index(tran)

    def __try_create_table(self, tran: Transaction) -> bool:
        tbl_name = self._query.get_table_name()
        statement = self._tables_defs.get(tbl_name, None)
        if statement is not None:
            tran.executescript(statement)
            return True
        return False

    def _save_table(self, tran: Transaction, tbl_exists: bool):
        script = self._query.get_script()
        if params := self._query.get_parameters():
            tran.executemany(script, params)
            return
        tran.executescript(script)

    def __try_create_index(self, tran: Transaction):
        tbl_name = self._query.get_table_name()
        statements = self._create_index_statements.get(tbl_name, None)
        if statements is not None:
            for statement in statements:
                tran.executescript(statement)


class DFToCRUDQueryMapper(NamedTuple):
    tbl_name: str
    df: DataFrame

    def get_table_name(self):
        return self.tbl_name

    def get_script(self) -> DataFrame:
        return self.df

    def get_parameters(self):
        raise NotImplementedError


class SaveTableOperationDF(SaveTableOperation):

    def _save_table(self, tran: Transaction, tbl_exists: bool):
        df = self._query.get_script()
        df.to_sql(
            name=self._query.get_table_name(),
            con=tran,
            if_exists='append' if tbl_exists else 'replace',
            index=False,
        )
