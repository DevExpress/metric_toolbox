import sqlite3
import os
from collections.abc import Mapping, Iterable
from pandas import DataFrame
from toolbox.sql.crud_queries.protocols import CRUDQuery


_db = None


def get_or_create_db():
    global _db
    if _db is None:
        _db = SQLiteDataBase()
    return _db


class SQLiteDataBase:

    def begin(self):
        return sqlite3.connect(database=os.environ['SQLITE_DATABASE'])

    def save_tables(
        self,
        tables: Mapping[str, DataFrame | CRUDQuery],
        tables_defs: Mapping[str, str] = {},
        create_index_statements: Mapping[str, str] = {},
    ) -> None:
        with self.begin() as conn:
            if tables is not None:
                for table_name, data in tables.items():
                    table_created_manually = self.try_create_table(
                        table=table_name,
                        tables_defs=tables_defs,
                        conn=conn,
                    )

                    if table_created_manually:
                        self.try_create_index(
                            table=table_name,
                            create_index_statements=create_index_statements,
                            conn=conn,
                        )

                    self.upsert_data(
                        conn=conn,
                        table_name=table_name,
                        data=data,
                        table_created_manually=table_created_manually,
                    )

                    if not table_created_manually:
                        self.try_create_index(
                            table=table_name,
                            create_index_statements=create_index_statements,
                            conn=conn,
                        )

    def upsert_data(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        data: DataFrame | CRUDQuery,
        table_created_manually: bool,
    ):
        match data:
            case DataFrame():
                data.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append' if table_created_manually else 'replace',
                    index=False,
                )
            case CRUDQuery():
                conn.executemany(data.get_script(), data.get_parameters())
                

    def try_create_table(
        self,
        table: str,
        tables_defs: Mapping[str, str],
        conn: sqlite3.Connection,
    ) -> bool:
        statement = tables_defs.get(table, None)
        if statement is not None:
            conn.executescript(statement)
            return True
        return False

    def try_create_index(
        self,
        table: str,
        create_index_statements: Mapping[str, Iterable[str]],
        conn: sqlite3.Connection,
    ):
        statements = create_index_statements.get(table, None)
        if statements is not None:
            for statement in statements:
                conn.executescript(statement)
