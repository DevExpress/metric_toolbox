import sqlite3
import os
from pandas import DataFrame
from typing import Dict


_db = None


def get_or_create_db():
    global _db
    if _db is None:
        _db = SQLiteDataBase()
    return _db


class SQLiteDataBase:

    def save_tables(self, tables: Dict[str, DataFrame]) -> None:
        with sqlite3.connect(database=os.environ['SQLITE_DATABASE']) as conn:
            if tables is not None:
                for k, v in tables.items():
                    v.to_sql(
                        name=k,
                        con=conn,
                        if_exists='replace',
                        index=False,
                    )
