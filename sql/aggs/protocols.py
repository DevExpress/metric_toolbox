from typing import Protocol, runtime_checkable

@runtime_checkable
class Expression(Protocol):

    def __str__(self) -> str:
        pass
