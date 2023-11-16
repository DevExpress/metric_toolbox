from collections.abc import Callable
from typing import Any


class FilterOp:

    def __init__(self, op: str, format: Callable = lambda x: x) -> None:
        self.op = op
        self.format = format

    def __str__(self) -> str:
        """Returns filter str representation."""
        return self.op

    def __call__(self, args: Any) -> Any:
        """Formats passed values."""
        return self.format(args)


between = FilterOp('between')
notbetwen = FilterOp('notbetween')
eq = FilterOp('=')
ne = FilterOp('!=')
ge = FilterOp('>=')
lt = FilterOp('<')
right_half_open = FilterOp('<=<', lambda x: f'[ {", ".join(str(v) for v in x)} )')
not_right_half_open = FilterOp('>=>', lambda x: f'( {", ".join(str(v) for v in x)} ]')
in_ = FilterOp('in')
notin = FilterOp('notin')
