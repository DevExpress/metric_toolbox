import sqlite3
import os
from pandas import DataFrame
from typing import Dict, Iterable


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
        tables: Dict[str, DataFrame],
        tables_defs: Dict[str, str] = {},
        create_index_statements: Dict[str, str] = {},
    ) -> None:
        with self.begin() as conn:
            if tables is not None:
                for k, v in tables.items():
                    table_created_manually = self.try_create_table(
                        table=k,
                        tables_defs=tables_defs,
                        conn=conn,
                    )
                    v.to_sql(
                        name=k,
                        con=conn,
                        if_exists='append' if table_created_manually else 'replace',
                        index=False,
                    )
                    self.try_create_index(
                        table=k,
                        create_index_statements=create_index_statements,
                        conn=conn,
                    )

    def try_create_table(
        self,
        table: str,
        tables_defs: Dict[str, str],
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
        create_index_statements: Dict[str, Iterable[str]],
        conn: sqlite3.Connection,
    ):
        statements = create_index_statements.get(table, None)
        if statements is not None:
            for statement in statements:
                conn.executescript(statement)
