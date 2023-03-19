from __future__ import annotations
from typing import Protocol


class Connection:

    def __init__(self, db_engine: DbEngine) -> None:
        self.db_engine = db_engine

    def begin_transaction(self) -> Transaction:
        return self.db_engine.begin()


class Transaction(Protocol):

    def __enter__(self):
        pass

    def __exit__(self, *kargs, **kwargs):
        pass

    def execute(self, *kargs, **kwargs):
        pass

    def executescript(self, *kargs, **kwargs):
        pass


class DbEngine(Protocol):

    def begin(self) -> Transaction:
        pass


class Connectable(Protocol):

    def get_connection_object(self) -> Connection:
        ...


class DbConnectable:

    def __init__(self, conn: Connection) -> None:
        self.__conn = conn

    def get_connection_object(self) -> Connection:
        return self.__conn
