from abc import ABC, abstractmethod
from typing import Protocol


class Transaction(Protocol):

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, *kargs, **kwargs):
        pass

    @abstractmethod
    def execute(self, *kargs, **kwargs):
        pass


class DbEngine(Protocol):

    def begin(self) -> Transaction:
        pass


class Connection(ABC):

    @abstractmethod
    def _get_or_create_engine(self) -> DbEngine:
        pass

    def begin_transaction(self) -> Transaction:
        return self._get_or_create_engine().begin()
