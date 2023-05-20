from collections.abc import Iterable, Sequence
from typing import Protocol, runtime_checkable


@runtime_checkable
class CRUDQuery(Protocol):

    def get_table_name(self) -> str:
        pass

    def get_script(self) -> str:
        pass

    def get_parameters(self) -> Iterable[Sequence]:
        pass
