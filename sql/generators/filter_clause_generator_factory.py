from collections.abc import Collection
from typing import Protocol, Any, runtime_checkable
from wrapt import decorator
import toolbox.sql.generators.filter_clause_generator as SqlFilterClauseGenerator



class BaseNode(Protocol):

    def get_field_values(self) -> dict[str, Any]:
        ...

    def get_field_alias(self, field_name) -> str:
        ...

    def get_append_operator(self) -> str:
        ...

    def get_filter_op(self, field_name) -> str:
        ...


class SupportsBool(Protocol):

    def __bool__(self) -> bool:
        ...


@runtime_checkable
class FilterParametersNode(BaseNode, SupportsBool, Protocol):
    include: bool
    values: Collection


@runtime_checkable
class FilterParameterNode(BaseNode, SupportsBool, Protocol):
    include: bool
    value: int


@decorator
def params_guard(func, instance, args, kwargs):
    if any(arg is None for arg in kwargs.values()):
        return ''
    return func(**kwargs)


class SqlFilterClauseFromFilterParametersGeneratorFactory:

    def get_like_filter_generator(positive: SupportsBool, /):
        if positive:
            return SqlFilterClauseGenerator.generate_like_filter
        return SqlFilterClauseGenerator.generate_not_like_filter

    def get_in_filter_generator(positive: SupportsBool, /):
        if positive:
            return SqlFilterClauseGenerator.generate_in_filter
        return SqlFilterClauseGenerator.generate_not_in_filter

    def get_between_filter_generator(positive: SupportsBool, /):
        if positive:
            return SqlFilterClauseGenerator.generate_between_filter
        return SqlFilterClauseGenerator.generate_not_between_filter

    def get_right_halfopen_interval_filter_generator(positive: SupportsBool, /):
        if positive:
            return SqlFilterClauseGenerator.generate_right_halfopen_interval_filter
        return SqlFilterClauseGenerator.generate_exclude_right_halfopen_interval_filter

    def get_equals_filter_generator(positive: SupportsBool, /):
        if positive:
            return SqlFilterClauseGenerator.generate_equals_filter
        return SqlFilterClauseGenerator.generate_not_equals_filter

    def get_null_filter_generator(positive: SupportsBool, /):
        if positive:
            return SqlFilterClauseGenerator.generate_is_null_filter
        return SqlFilterClauseGenerator.generate_is_not_null_filter

    def get_not_null_filter_generator(positive: SupportsBool, /):
        return SqlFilterClauseFromFilterParametersGeneratorFactory.get_null_filter_generator(not positive)

    def get_less_equals_filter_generator(positive: SupportsBool, /):
        if positive:
            return SqlFilterClauseGenerator.generate_less_equals_filter
        return SqlFilterClauseGenerator.generate_not_less_equals_filter

    def get_less_filter_generator(positive: SupportsBool, /):
        if positive:
            return SqlFilterClauseGenerator.generate_less_filter
        return SqlFilterClauseGenerator.generate_not_less_filter
