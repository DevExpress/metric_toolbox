from typing import Union


TEXT = 'TEXT'
INTEGER = 'INTEGER'
REAL = 'REAL'
NUMERIC = 'NUMERIC'


class Field:
    __slots__ = ('name', 'type')

    def __init__(
        self,
        type: Union[TEXT, INTEGER, REAL, NUMERIC] = TEXT,
        *,
        alias: str | None = None,
    ) -> None:
        self.type = type
        self.name = alias

    def __str__(self) -> str:
        return self.name

    def __set_name__(self, owner, name):
        if self.name:
            return
        self.name = name
