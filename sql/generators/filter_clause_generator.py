from collections.abc import Iterable, Callable
from typing import Any
from toolbox.utils.converters import to_quoted_string
from toolbox.sql.generators.decorators import (
    include_filter,
    exclude_filter,
    null_only,
    not_null_only,
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


@include_filter
def generate_right_halfopen_interval_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    start, end, *_ = values
    filter = f'{values_converter(start)} <= {col} AND {col} < {values_converter(end)}'
    return parenthesize_if_orfilter(kwargs, filter)


@exclude_filter
def generate_exclude_right_halfopen_interval_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    start, end, *_ = values
    filter = f'{values_converter(start)} > {col} AND {col} >= {values_converter(end)}'
    return parenthesize_if_orfilter(kwargs, filter)


@include_filter
def generate_equals_filter(
    *,
    col: str,
    value: Any,
    value_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    return f'{col} = {value_converter(value)}'


@exclude_filter
def generate_not_equals_filter(
    *,
    col: str,
    value: Any,
    value_converter: Callable[[Any], str] = to_quoted_string,
    **kwargs,
):
    return f'{col} != {value_converter(value)}'


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
    start, end, *_ = values
    return f'BETWEEN {values_converter(start)} AND {values_converter(end)}'


def parenthesize_if_orfilter(kwargs: dict, filter: str) -> str:
    return f'({filter})' if kwargs.get('or_filter', None) else filter
