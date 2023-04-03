from collections.abc import Iterable, Callable
from wrapt import decorator
import functools
from toolbox.sql.generators import NULL_FILTER_VALUE


def include_filter(
    base_filter: Callable[..., str] = None,
    *,
    ignore_values: bool = False,
):
    if base_filter is None:
        return functools.partial(include_filter, ignore_values=ignore_values)

    @decorator
    def include_filter_inner(
        base_filter: Callable[..., str],
        instance,
        args: Iterable,
        kwargs: dict,
    ):
        if (values := kwargs.get('values', None)) or ignore_values:
            filter_prefix = __try_add_space(kwargs['filter_prefix'])

            if values is not None and NULL_FILTER_VALUE in values:
                isnull_filter = f"{kwargs['col']} IS NULL"
                return __generate_positive_isnull_filter(
                    values,
                    kwargs,
                    filter_prefix,
                    isnull_filter,
                    base_filter,
                )

            return __generate_filter(filter_prefix, base_filter(**kwargs))
        return ''

    return include_filter_inner(base_filter)


@decorator
def exclude_filter(
    base_filter: Callable[..., str],
    instance,
    args: Iterable,
    kwargs: dict,
):
    col = kwargs['col']

    isnull_filter = f'{col} IS NULL'
    filter_prefix = __try_add_space(kwargs['filter_prefix'])

    if values := kwargs.get('values', None):

        if NULL_FILTER_VALUE in values:
            isnull_filter = f'{col} IS NOT NULL'
            return __generate_negative_isnull_filter(
                values,
                kwargs,
                filter_prefix,
                isnull_filter,
                base_filter,
            )

        return __generate_or_filter(
            kwargs,
            filter_prefix,
            isnull_filter,
            base_filter,
        )
    return __generate_filter(filter_prefix, isnull_filter)


def __try_add_space(prefix):
    if prefix:
        return prefix + ' '
    return prefix


def __generate_positive_isnull_filter(
    values: Iterable,
    kwargs: dict,
    filter_prefix: str,
    isnull_filter: str,
    base_filter_func: Callable[..., str],
):
    if len(values) > 1:
        kwargs['values'] = (val for val in values if val != NULL_FILTER_VALUE)
        return __generate_or_filter(
            kwargs,
            filter_prefix,
            isnull_filter,
            base_filter_func,
        )
    return __generate_filter(filter_prefix, isnull_filter)


def __generate_negative_isnull_filter(
    values: Iterable,
    kwargs: dict,
    filter_prefix: str,
    isnull_filter: str,
    base_filter_func: Callable[..., str],
):
    if len(values) > 1:
        kwargs['values'] = (val for val in values if val != NULL_FILTER_VALUE)
        return __generate_and_filter(
            kwargs,
            filter_prefix,
            isnull_filter,
            base_filter_func,
        )
    return __generate_filter(filter_prefix, isnull_filter)


def __generate_or_filter(
    kwargs: dict,
    filter_prefix: str,
    isnull_filter: str,
    base_filter_func: Callable[..., str],
):
    return __generate_filter(
        filter_prefix,
        f'({isnull_filter} OR {base_filter_func(**kwargs)})',
    )


def __generate_and_filter(
    kwargs: dict,
    filter_prefix: str,
    isnull_filter: str,
    base_filter_func: Callable[..., str],
):
    return __generate_filter(
        filter_prefix,
        f'({isnull_filter} AND {base_filter_func(**kwargs)})',
    )


def __generate_filter(filter_prefix: str, filter: str):
    return filter_prefix + filter
