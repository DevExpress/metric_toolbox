from collections.abc import Collection
from typing import Protocol, Any, runtime_checkable
import toolbox.sql.generators.filter_clause_generator as SqlFilterClauseGenerator
from wrapt import decorator


class BaseNode(Protocol):

    def get_field_values(self) -> dict[str, Any]:
        ...

    def get_field_alias(self, field_name) -> str:
        ...

    def get_append_operator(self) -> str:
        ...

    def get_filter_op(self, field_name) -> str:
        ...


@runtime_checkable
class FilterParametersNode(BaseNode, Protocol):
    include: bool
    values: Collection


@runtime_checkable
class FilterParameterNode(BaseNode, Protocol):
    include: bool
    value: int


@decorator
def params_guard(func, instance, args, kwargs):
    if any(arg is None for arg in kwargs.values()):
        return ''
    return func(**kwargs)


class SqlFilterClauseFromFilterParametersGeneratorFactory:

    def get_like_filter_generator(params: FilterParametersNode):
        if params.include:
            return SqlFilterClauseGenerator.generate_like_filter
        return SqlFilterClauseGenerator.generate_not_like_filter

    def get_in_filter_generator(params: FilterParametersNode):
        if params.include:
            return SqlFilterClauseGenerator.generate_in_filter
        return SqlFilterClauseGenerator.generate_not_in_filter

    def get_between_filter_generator(params: FilterParametersNode):
        if params.include:
            return SqlFilterClauseGenerator.generate_between_filter
        return SqlFilterClauseGenerator.generate_not_between_filter
