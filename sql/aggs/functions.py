from collections.abc import Callable, Iterable
from typing import NamedTuple
from numbers import Number
from toolbox.sql.aggs.protocols import Expression


class Expr(NamedTuple):
    expr: str

    def __str__(self) -> str:
        return self.expr


class Func:

    def __init__(
        self,
        param: Expression,
        *expressions: Expression,
        op: str = '',
        iif_zero: Number = 0
    ) -> None:
        self._param = param
        self.expressions = expressions or self._func(param)
        self.op = op
        self._iif_zero = iif_zero

    def _as_str(
        self,
        window: str = '',
        format: Callable[[Expression], str] = lambda x: str(x),
    ) -> str:
        res = self.op.join(format(expr) for expr in self.expressions)
        if len(self.expressions) > 1 and self.op == ' + ':
            return f'({res})'
        return res

    def __str__(self) -> str:
        return self._as_str()

    def __repr__(self) -> str:
        return self.__str__()

    def __add__(self, other: Expression) -> 'Func':
        return Func('', self, other, op=' + ')

    def __mul__(self, other: Expression) -> 'Func':
        return Func('', self, other, op=' * ')

    def __truediv__(self, other: Expression) -> 'Func':
        return DIV(self, other)

    def __eq__(self, other: Expression) -> bool:
        return str(self) == str(other)

    def _func(self, param: str) -> Iterable[Expression]:
        raise NotImplementedError

    def over(self, window: str) -> str:

        def format(x: Expression):
            match x:
                case str():
                   return x
                case Func():
                   return x.over(window)
            return f'{x} OVER ({window})'

        return self._as_str(
            window,
            format,
        )

    def iif_zero(self):
        return self._iif_zero


class DIV(Func):

    def __init__(
        self, dividee: Func | Expression, divider: Func | Expression
    ) -> None:
        self.dividee = dividee
        self.divider = divider

    def _as_str(self, window: str = '', *_):
        # yapf: disable
        dividee, divider = self.dividee, self.divider
        if window:
            if hasattr(dividee, 'over'):
                dividee= self.dividee.over(window)
            if hasattr(divider, 'over'):
                divider = self.divider.over(window)
        zero = divider.iif_zero() if hasattr(divider, 'iif_zero') else 0
        return f'IIF({divider} = 0, {zero}, {dividee} * 1. / {divider})'
        # yapf: enable


class SUM(Func):

    def _func(self, param: Expression) -> Iterable[Expression]:
        return Expr(f'SUM({param})'),


class COUNT(Func):

    def _func(self, param: Expression) -> Iterable[Expression]:
        return Expr(f'COUNT({param})'),


class COUNT_DISTINCT(Func):

    def _func(self, param: Expression) -> Iterable[Expression]:
        return Expr(f'COUNT(DISTINCT {param})'),

    def over(self, window: str) -> str:
        return (
            f'(DENSE_RANK() OVER ({window} ORDER BY {self._param}) + '
            + f'DENSE_RANK() OVER ({window} ORDER BY {self._param} DESC) - ' +
            f'SUM(IIF({self._param} IS NULL, 1, 0)) OVER ({window} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 1)'
        )


class AVG(Func):

    def _func(self, param: Expression) -> Iterable[Expression]:
        return Expr(f'AVG({param})'),
