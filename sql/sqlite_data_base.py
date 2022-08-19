import sqlite3
from pandas import DataFrame
from typing import Any, Dict


class SQLiteDataBase:

    def __init__(
        self,
        name: str = None,
    ):
        self.data_base = name
        self._connection: sqlite3.Connection = None
        self._connections_count: int = 0

    def connect(self) -> None:
        self.disconnect()
        self._connection = sqlite3.connect(database=self.data_base)

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def try_connect(self) -> None:
        self._add_connection()
        if not self._connection:
            self.connect()

    def try_disconnect(self) -> None:
        self._remove_connection()
        if self._all_connections_removed():
            self.disconnect()

    def _add_connection(self) -> None:
        self._connections_count += 1

    def _remove_connection(self) -> None:
        self._connections_count -= 1
        if self._connections_count < 0:
            self._connections_count = 0

    def _all_connections_removed(self) -> bool:
        return not self._connections_count

    def get_connection(self) -> Any:
        return self._connection

    def save_tables(self, tables: Dict[str, DataFrame]) -> None:
        self.try_connect()
        if tables is not None:
            for k, v in tables.items():
                v.to_sql(
                    name=k,
                    con=self._connection,
                    if_exists='replace',
                    index=False,
                )
        self.try_disconnect()
