from typing import Callable, Any, Iterable, Optional
from toolbox.utils.decorator_factory import decorator


def include_filter(ignore_values: bool = False):

    @decorator
    def include_filter_inner(
        base_filter: Callable[..., str],
        instance: Any,
        filter_prefix: str,
        col: str,
        values: Optional[Iterable] = None,
        values_converter: Optional[Callable[[Any], str]] = None,
    ):
        if values or ignore_values:
            filter_prefix = _try_add_space(filter_prefix)
            return filter_prefix + base_filter(**locals())
        return ''

    return include_filter_inner


@decorator
def exclude_filter(
    base_filter: Callable[..., str],
    instance: Any,
    filter_prefix: str,
    col: str,
    values: Optional[Iterable] = None,
    values_converter: Optional[Callable[[Any], str]] = None,
):
    isnull_filter = f'{col} IS NULL'
    filter_prefix = _try_add_space(filter_prefix)
    if values:
        return f'{filter_prefix}({isnull_filter} OR {base_filter(**locals())})'
    return filter_prefix + isnull_filter


def _try_add_space(prefix):
    if prefix:
        return prefix + ' '
    return prefix


@include_filter()
def generate_in_filter(
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str],
    **_,
) -> str:
    return f'{col} IN ({in_values(values, values_converter)})'


@exclude_filter
def generate_not_in_filter(
    col: str,
    values: Iterable,
    values_converter: Callable[[Any], str],
    **_,
) -> str:
    return f'{col} NOT IN ({in_values(values, values_converter)})'


@include_filter()
def generate_like_filter(col: str, values: Iterable, **_):
    return f'({like(col, values)})'


@exclude_filter
def generate_not_like_filter(col: str, values: Iterable, **_):
    return f'NOT ({like(col, values)})'


@include_filter(ignore_values=True)
def generate_is_not_null_filter(col: str, **_) -> str:
    return f'{col} IS NOT NULL'


def in_values(values: Iterable, values_converter: Callable[[Any], str]):
    return ','.join([values_converter(val) for val in values])


def like(col: str, values: Iterable):
    return ' OR '.join([f"{col} LIKE '%{value}%'" for value in values])
