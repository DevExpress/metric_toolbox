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
            res = f'{col} IN ('
            res += ','.join([values_converter(val) for val in values])
            res += ')'
            return res

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
            res = f'({col} IS NULL OR {col} NOT IN ('
            res += ','.join([values_converter(val) for val in values])
            res += '))'
            return res

        return self._generate_filter(
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
            res = '('
            res += ' OR '.join([f"{col} LIKE '%{value}%'" for value in values])
            res += ')'
            return res

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
            res = f'({col} IS NULL OR ('
            res += ' AND '.join([f"{col} NOT LIKE '%{value}%'" for value in values])
            res += '))'
            return res

        return self._generate_filter(
            values=values,
            filter_prefix=filter_prefix,
            get_filter=filter_func,
        )

    def _generate_filter(
        self,
        values: list,
        filter_prefix: str,
        get_filter: Callable[[], str],
    ) -> str:
        if not values:
            return ''
        actual_filter = get_filter()
        return filter_prefix + actual_filter
