from typing import NamedTuple
from toolbox.sql.aggs.functions import Func


class Metric(NamedTuple):
    name: str
    expression: Func

    def __str__(self) -> str:
        return str(self.expression)

    def __add__(self, other: 'Metric'):
        return Metric('', self.expression + other.expression)

    def __mul__(self, other: 'Metric'):
        return Metric('', self.expression * other.expression)

    def __truediv__(self, other: 'Metric'):
        return Metric('', self.expression / other.expression)

    def __eq__(self, other: 'Metric') -> bool:
        return str(self) == str(other)

    @classmethod
    def from_metric(cls, name: str, metric: 'Metric'):
        return cls(name, metric.expression)

    def get_over(self, window: str) -> str:
        return self.expression.over(window)
