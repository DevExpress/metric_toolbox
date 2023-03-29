from collections.abc import Iterable, Callable
from typing import Any
from wrapt import decorator
import functools
from toolbox.utils.converters import to_quoted_string


def include_filter(
    base_filter: Callable[..., str] = None,
    *,
    ignore_values: bool = False,
):
    if base_filter is None:
        return functools.partial(include_filter, ignore_values=ignore_values)

    @decorator
    def include_filter_inner(base_filter: Callable[..., str], instance, args, kwargs):
        if kwargs.get('values', None) or ignore_values:
            filter_prefix = _try_add_space(kwargs['filter_prefix'])
            return filter_prefix + base_filter(**kwargs)
        return ''

    return include_filter_inner(base_filter)


@decorator
def exclude_filter(base_filter: Callable[..., str], instance, args, kwargs):
    isnull_filter = f"{kwargs['col']} IS NULL"
    filter_prefix = _try_add_space(kwargs['filter_prefix'])
    if kwargs.get('values', None):
        return f'{filter_prefix}({isnull_filter} OR {base_filter(**kwargs)})'
    return filter_prefix + isnull_filter


def _try_add_space(prefix):
    if prefix:
        return prefix + ' '
    return prefix


@include_filter
def generate_in_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str]=to_quoted_string,
    **_,
) -> str:
    return f'{col} IN ({in_values(values, values_converter)})'


@exclude_filter
def generate_not_in_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str]=to_quoted_string,
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
    values_converter: Callable[[Any], str]=to_quoted_string,
    **_,
):
    return f'{col} {between(values,values_converter)}'


@exclude_filter
def generate_not_between_filter(
    *,
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str]=to_quoted_string,
    **_,
):
    return f'{col} NOT {between(values,values_converter)}'


@include_filter(ignore_values=True)
def generate_is_not_null_filter(*, col: str, **_) -> str:
    return f'{col} IS NOT NULL'


def in_values(values: Iterable, values_converter: Callable[[Any], str]):
    return ','.join([values_converter(val) for val in values])


def like(col: str, values: Iterable):
    return ' OR '.join([f"{col} LIKE '%{value}%'" for value in values])


def between(values: Iterable, values_converter: Callable[[Any], str]):
    start, end, *_ = values
    return f'BETWEEN {values_converter(start)} AND {values_converter(end)}'
