from typing import Protocol


class Expression(Protocol):

    def __str__(self) -> str:
        pass
