from collections.abc import Iterable, Callable
from typing import Any
from toolbox.utils.converters import to_quoted_string
from toolbox.sql.generators.decorators import (
    include_filter,
    exclude_filter,
    null_only,
    not_null_only,
    not_null,
    not_null_exclude,
)


@include_filter
def generate_in_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str] = to_quoted_string,
    **_,
) -> str:
    return f'{col} IN ({in_values(values, values_converter)})'


@exclude_filter
def generate_not_in_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str] = to_quoted_string,
    **_,
) -> str:
    return f'{col} NOT IN ({in_values(values, values_converter)})'


@include_filter
def generate_like_filter(*, col: str, values: Iterable, **_):
    return f'({like(col, values)})'


@exclude_filter
def generate_not_like_filter(*, col: str, values: Iterable, **_):
    return f'NOT ({like(col, values)})'


@include_filter
def generate_between_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str] = to_quoted_string,
    **_,
):
    return f'{col} {between(values, values_converter)}'


@exclude_filter
def generate_not_between_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str] = to_quoted_string,
    **_,
):
    return f'{col} NOT {between(values, values_converter)}'


@not_null
def generate_right_halfopen_interval_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    start, end, *_ = map(values_converter, values)
    filter = f'{le(start, col)} AND {lt(col, end)}'
    return parenthesize_if_orfilter(kwargs, filter)


@exclude_filter
def generate_exclude_right_halfopen_interval_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    start, end, *_ = map(values_converter, values)
    filter = f'{gt(start, col)} AND {ge(col, end)}'
    return parenthesize_if_orfilter(kwargs, filter)


@include_filter
def generate_equals_filter(
    *,
    col: str,
    value: Any,
    value_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    return eq(col, value_converter(value))


@exclude_filter
def generate_not_equals_filter(
    *,
    col: str,
    value: Any,
    value_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    return ne(col, value_converter(value))


@not_null
def generate_less_equals_filter(
    *,
    col: str,
    value: Any,
    value_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    return le(col, value_converter(value))


@not_null_exclude
def generate_not_less_equals_filter(
    *,
    col: str,
    value: Any,
    value_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    return gt(col, value_converter(value))


@not_null
def generate_less_filter(
    *,
    col: str,
    value: Any,
    value_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    return lt(col, value_converter(value))


@not_null_exclude
def generate_not_less_filter(
    *,
    col: str,
    value: Any,
    value_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    return ge(col, value_converter(value))


@not_null_only
def generate_is_not_null_filter(*, col: str, **_) -> str:
    ...


@null_only
def generate_is_null_filter(*, col: str, **_) -> str:
    ...


def in_values(values: Iterable, values_converter: Callable[[Any], str]):
    return ','.join(values_converter(val) for val in values)


def like(col: str, values: Iterable):
    return ' OR '.join(f"{col} LIKE '%{value}%'" for value in values)


def between(values: Iterable, values_converter: Callable[[Any], str]):
    start, end, *_ = map(values_converter, values)
    return f'BETWEEN {start} AND {end}'


def eq(col: str, value: Any):
    return f'{col} = {value}'


def ne(col: str, value: Any):
    return f'{col} != {value}'


def lt(col: str, value: Any):
    return f'{col} < {value}'


def le(col: str, value: Any):
    return f'{col} <= {value}'


def gt(col: str, value: Any):
    return f'{col} > {value}'


def ge(col: str, value: Any):
    return f'{col} >= {value}'


def parenthesize_if_orfilter(kwargs: dict, filter: str) -> str:
    return f'({filter})' if kwargs.get('or_filter', None) else filter
