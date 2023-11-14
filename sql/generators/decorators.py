from collections.abc import Iterable, Callable
from wrapt import decorator
from toolbox.sql.generators import NULL_FILTER_VALUE


@decorator
def null_only(
    base_filter: Callable[..., str],
    instance,
    args: Iterable,
    kwargs: dict,
):
    return __generate_filter(
        kwargs,
        __generate_is_null_filter(kwargs),
    )


@decorator
def not_null_only(
    base_filter: Callable[..., str],
    instance,
    args: Iterable,
    kwargs: dict,
):
    return __generate_filter(
        kwargs,
        __generate_is_not_null_filter(kwargs),
    )


@decorator
def not_null(
    base_filter: Callable[..., str],
    instance,
    args: Iterable,
    kwargs: dict,
):
    return __include_filter(base_filter, kwargs, True)


@decorator
def include_filter(
    base_filter: Callable[..., str],
    instance,
    args: Iterable,
    kwargs: dict,
):
    return __include_filter(base_filter, kwargs)


@decorator
def exclude_filter(
    base_filter: Callable[..., str],
    instance,
    args: Iterable,
    kwargs: dict,
):
    if __should_generate_null_filter(kwargs):
        return __generate_is_not_null_and_base_filter(
            kwargs,
            base_filter,
        )

    return __generate_is_null_or_base_filter(
        kwargs,
        base_filter,
    )


def __include_filter(
    base_filter: Callable[..., str],
    kwargs: dict,
    not_null: bool = False,
):
    values = __get_real_values(kwargs)
    if values is not None:
        if __should_generate_null_filter(kwargs):
            return __generate_is_null_or_base_filter(
                kwargs,
                base_filter,
            )

        if not_null:
            return __generate_is_not_null_and_base_filter(
                kwargs,
                base_filter,
            )

        return __generate_filter(kwargs, base_filter(**kwargs))
    return ''


def __generate_is_null_filter(kwargs: dict):
    return f'{__get_col(kwargs)} IS NULL'


def __generate_is_not_null_filter(kwargs: dict):
    return f'{__get_col(kwargs)} IS NOT NULL'


def __get_col(kwargs):
    return kwargs['col']


def __get_real_values(kwargs: dict) -> Iterable | None:
    if (values := __get_values(kwargs)) is not None:
        return [val for val in values if val != NULL_FILTER_VALUE]
    return None


def __get_values(kwargs: dict) -> Iterable | None:
    if (values := kwargs.get('values', None)) and issubclass(type(values), Iterable):
        return values
    if (value := kwargs.get('value', None)) is not None:
        return (value, )
    return None


def __update_and_get_real_values(kwargs: dict) -> Iterable | None:
    values = __get_real_values(kwargs)
    kwargs['values'] = values
    return values


def __should_generate_null_filter(kwargs: dict):
    if values := __get_values(kwargs):
        return NULL_FILTER_VALUE in values
    return None


def __generate_is_null_or_base_filter(
    kwargs: dict,
    base_filter_func: Callable[..., str],
):
    filter = __generate_is_null_filter(kwargs)
    values = __update_and_get_real_values(kwargs)
    if values:
        filter = f'({filter} OR {base_filter_func(or_filter=True, **kwargs)})'
    return __generate_filter(kwargs, filter)


def __generate_is_not_null_and_base_filter(
    kwargs: dict,
    base_filter_func: Callable[..., str],
):
    filter = __generate_is_not_null_filter(kwargs)
    values = __update_and_get_real_values(kwargs)
    if values:
        filter = f'({filter} AND {base_filter_func(or_filter=False, **kwargs)})'
    return __generate_filter(kwargs, filter)


def __generate_filter(kwargs: dict, filter: str | None):
    if filter:
        return __get_filter_prefix(kwargs) + filter
    return ''


def __get_filter_prefix(kwargs: dict):
    prefix = kwargs['filter_prefix']
    if prefix:
        return prefix + ' '
    return prefix
