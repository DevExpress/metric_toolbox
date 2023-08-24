from typing import NamedTuple
from toolbox.sql.aggs.functions import Func
from toolbox.sql.aggs.protocols import Expression


class Metric(NamedTuple):
    name: str
    display_name: str
    group: str
    expression: Func

    def __str__(self) -> str:
        return str(self.expression)

    def __add__(self, other: Expression):
        return Metric('', '', '', self.expression + self.__to_expression(other))

    def __mul__(self, other: Expression):
        return Metric('', '', '', self.expression * self.__to_expression(other))

    def __truediv__(self, other: Expression):
        return Metric('', '', '', self.expression / self.__to_expression(other))

    def __to_expression(self, other):
        if isinstance(other, Metric):
            other = other.expression
        return other

    def __eq__(self, other: Expression) -> bool:
        return str(self) == str(other)

    def get_display_name(self) -> str:
        return self.display_name or self.name

    @classmethod
    def from_metric(
        cls,
        name: str,
        displayName: str,
        group: str,
        metric: 'Metric',
    ) -> 'Metric':
        return cls(name, displayName, group, metric.expression)

    def get_over(self, window: str) -> str:
        return self.expression.over(window)
