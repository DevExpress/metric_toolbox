from typing import Protocol
from abc import abstractmethod


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
