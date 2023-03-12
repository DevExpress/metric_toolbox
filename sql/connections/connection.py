from typing import Protocol


class Transaction(Protocol):

    def __enter__(self):
        pass

    def __exit__(self, *kargs, **kwargs):
        pass

    def execute(self, *kargs, **kwargs):
        pass


class DbEngine(Protocol):

    def begin(self) -> Transaction:
        pass


class Connection:

    def __init__(self, db_engine: DbEngine) -> None:
        self.db_engine = db_engine

    def begin_transaction(self) -> Transaction:
        return self.db_engine.begin()
