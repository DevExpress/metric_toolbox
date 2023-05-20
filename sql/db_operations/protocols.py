from toolbox.sql.connections.connection import Transaction
from typing import Protocol


class DBOperation(Protocol):

    def __call__(self, tran: Transaction):
        pass
