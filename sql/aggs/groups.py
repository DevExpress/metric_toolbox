from typing import NamedTuple


class GroupBy(NamedTuple):
    expression: str
    statement: str


class Window(NamedTuple):
    name: str
    statement: str
