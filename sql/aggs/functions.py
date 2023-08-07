from collections.abc import Callable, Iterable
from typing import NoReturn


class Func:

    def __init__(self, param: str, *expressions: 'Func', op: str = '') -> None:
        self.expressions = expressions or self._func(param)
        self.op = op

    def _as_str(
        self,
        window: str = '',
        format: Callable[['Func'], str] = lambda x: str(x),
    ) -> str:
        res = self.op.join(format(expr) for expr in self.expressions)
        if len(self.expressions) > 1 and self.op == ' + ':
            return f'({res})'
        return res

    def __str__(self) -> str:
        return self._as_str()

    def __repr__(self) -> str:
        return self.__str__()

    def __add__(self, other: 'Func') -> 'Func':
        return SUM('', self, other, op=' + ')

    def __mul__(self, other) -> 'Func':
        return SUM('', self, other, op=' * ')

    def __truediv__(self, other: 'Func') -> 'Func':
        return DIV(self, other)

    def __eq__(self, other: 'Func') -> bool:
        return str(self) == str(other)

    def over(self, window: str) -> str:
        return self._as_str(window, lambda x: f'{x} OVER ({window})')


class DIV(Func):

    def __init__(self, dividee: Func, divider: Func) -> None:
        self.dividee = dividee
        self.divider = divider

    def _as_str(self, window: str = '', *_):
        # yapf: disable
        dividee, divider = self.dividee, self.divider
        if window:
            dividee, divider = self.dividee.over(window), self.divider.over(window)
        return f'IIF({divider} = 0, 0, {dividee} * 1.0 / {divider})'
        # yapf: enable


class SUM(Func):

    def _func(self, param: str) -> Iterable[str]:
        return f'SUM({param})',


class COUNT(Func):

    def _func(self, param: str) -> Iterable[str]:
        return f'COUNT({param})',


class COUNT_DISTINCT(Func):

    def _func(self, param: str) -> Iterable[str]:
        return f'COUNT(DISTINCT {param})',

    def over(self, window: str) -> NoReturn:
        raise SyntaxError(
            'COUNT(DISTINCT ..) is not analytic func. You cannot use it with OVER.'
        )


class AVG(Func):

    def _func(self, param: str) -> Iterable[str]:
        return f'AVG({param})',
