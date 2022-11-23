from abc import ABC, abstractmethod
from toolbox.sql.query_executors.base.protocols import (
    DbEngine,
    Transaction,
)


class ConnectionObject(ABC):

    @abstractmethod
    def _get_or_create_engine(self) -> DbEngine:
        pass

    def begin_transaction(self) -> Transaction:
        return self._get_or_create_engine().begin()
