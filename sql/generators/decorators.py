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
        values, generate_null_filter = __select_real_values(kwargs)
        kwargs['values'] = values
        if values is not None or ignore_values:
            filter_prefix = __try_add_space(kwargs['filter_prefix'])

            if generate_null_filter:
                isnull_filter = f"{kwargs['col']} IS NULL"
                return __try_generate_or_filter(
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

    values, generate_null_filter = __select_real_values(kwargs)
    kwargs['values'] = values
    if values is not None:

        if generate_null_filter:
            isnull_filter = f'{col} IS NOT NULL'
            return __try_generate_and_filter(
                kwargs,
                filter_prefix,
                isnull_filter,
                base_filter,
            )

        return __try_generate_or_filter(
            kwargs,
            filter_prefix,
            isnull_filter,
            base_filter,
        )
    return __generate_filter(filter_prefix, isnull_filter)


def __select_real_values(kwargs: dict) -> tuple[Iterable, bool]:
    values = kwargs.get('values', None)
    if values:
        return [val for val in values if val != NULL_FILTER_VALUE], NULL_FILTER_VALUE in values
    return None, False


def __try_add_space(prefix):
    if prefix:
        return prefix + ' '
    return prefix


def __try_generate_or_filter(
    kwargs: dict,
    filter_prefix: str,
    isnull_filter: str,
    base_filter_func: Callable[..., str],
):
    filter = isnull_filter
    if kwargs['values']:
        filter = f'({isnull_filter} OR {base_filter_func(or_filter=True,**kwargs)})'
    return __generate_filter(filter_prefix, filter)


def __try_generate_and_filter(
    kwargs: dict,
    filter_prefix: str,
    isnull_filter: str,
    base_filter_func: Callable[..., str],
):
    filter = isnull_filter
    if kwargs['values']:
        filter = f'({isnull_filter} AND {base_filter_func(or_filter=False, **kwargs)})'
    return __generate_filter(filter_prefix, filter)


def __generate_filter(filter_prefix: str, filter: str):
    return filter_prefix + filter
