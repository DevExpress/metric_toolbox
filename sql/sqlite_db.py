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
        create_index_expressions: Dict[str, str] = {},
    ) -> None:
        with self.begin() as conn:
            if tables is not None:
                for k, v in tables.items():
                    v.to_sql(
                        name=k,
                        con=conn,
                        if_exists='replace',
                        index=False,
                    )
                    self.try_create_index(
                        table=k,
                        create_index_expressions=create_index_expressions,
                        conn=conn,
                    )

    def try_create_index(
        self,
        table: str,
        create_index_expressions: Dict[str, Iterable[str]],
        conn: sqlite3.Connection,
    ):
        exprs = create_index_expressions.get(table, None)
        if exprs is not None:
            for expr in exprs:
                conn.execute(expr)
