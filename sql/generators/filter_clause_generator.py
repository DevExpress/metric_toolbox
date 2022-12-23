from typing import Callable, Any


class SqlFilterClauseGenerator:

    def generate_in_filter(
        self,
        col: str,
        values: list,
        filter_prefix: str,
        values_converter: Callable[[Any], str],
    ) -> str:

        def filter_func():
            filter_values = ','.join([values_converter(val) for val in values])
            return f'{col} IN (' + filter_values + ')'

        return self._generate_filter(
            values=values,
            filter_prefix=filter_prefix,
            get_filter=filter_func,
        )

    def generate_not_in_filter(
        self,
        col: str,
        values: list,
        filter_prefix: str,
        values_converter: Callable[[Any], str],
    ) -> str:

        def filter_func():
            filter_values = ','.join([values_converter(val) for val in values])
            return f'{col} NOT IN (' + filter_values + ')'

        return self._generate_exclude_fitler(
            col=col,
            values=values,
            filter_prefix=filter_prefix,
            get_filter=filter_func,
        )

    def generate_like_filter(
        self,
        col: str,
        values: list,
        filter_prefix: str,
    ):

        def filter_func():
            filter = ' OR '.join([f"{col} LIKE '%{value}%'" for value in values])
            return f'({filter})'

        return self._generate_filter(
            values=values,
            filter_prefix=filter_prefix,
            get_filter=filter_func,
        )

    def generate_not_like_filter(
        self,
        col: str,
        values: list,
        filter_prefix: str,
    ):

        def filter_func():
            filter = ' OR '.join([f"{col} LIKE '%{value}%'" for value in values])
            return f'NOT ({filter})'

        return self._generate_exclude_fitler(
            col=col,
            values=values,
            filter_prefix=filter_prefix,
            get_filter=filter_func,
        )

    def _generate_exclude_fitler(
        self,
        col: str,
        values: list,
        filter_prefix: str,
        get_filter: Callable[[], str],
    ) -> str:
        is_null_fitler = f'{col} IS NULL'
        values_filter = self._generate_filter(
            values=values,
            filter_prefix='',
            get_filter=get_filter,
        )
        if values_filter:
            values_filter = ' OR' + values_filter
        return filter_prefix + ' (' + is_null_fitler + values_filter + ')'

    def _generate_filter(
        self,
        values: list,
        filter_prefix: str,
        get_filter: Callable[[], str],
    ) -> str:
        return (filter_prefix + ' ' + get_filter()) if values else ''
