from collections.abc import Mapping
from toolbox.sql.meta_data import MetaData
from toolbox.sql_async import AsyncSQLiteQueryExecutor
from toolbox.utils import json_array_of_values
from toolbox.sql_async.sql_query import AsyncSqlQuery


class PeriodsMeta(MetaData):
    start = 'start'
    period = 'period'


async def get_group_by_periods_json():
    # format should contain a valid strftime string.
    # https://sqlite.org/lang_datefunc.html
    return '''[
    { "name": "Day",        "format": "%Y-%m-%d" },
    { "name": "Week-Year",  "format": "%Y-%W" },
    { "name": "Month-Year", "format": "%Y-%m" },
    { "name": "Year",       "format": "%Y" }
]
'''


def generate_group_by_period(format: str, field: str) -> str:
    if format == '%Y-%W':
        return f"STRFTIME('%Y-%m-%d', {field}, 'WEEKDAY 0', '-6 DAYS')"
    return f"STRFTIME('{format}', {field})"


async def generate_periods(
    start: str,
    end: str,
    format: str,
) -> str:
    anchor_modifier = {
        '%Y-%m-%d': "'START OF DAY'",
        '%Y-%W': "'WEEKDAY 0', '-6 DAYS'",
        '%Y-%m': "'START OF MONTH'",
        '%Y': "'START OF YEAR'",
    }[format]

    recursive_member_modifier = {
        '%Y-%m-%d': "'+1 DAYS'",
        '%Y-%W': "'+7 DAYS'",
        '%Y-%m': "'+1 MONTHS'",
        '%Y': "'+1 YEAR'",
    }[format]

    if format == '%Y-%W':
        format = '%Y-%m-%d'

    # yapf: disable
    format_params = {
        **PeriodsMeta.get_attrs(),
        'anchor_expr': f"STRFTIME('%Y-%m-%d', '{start}', {anchor_modifier})",
        'anchor_expr_formatted': f"STRFTIME('{format}', '{start}', {anchor_modifier})",
        'recursive_expr': f"STRFTIME('%Y-%m-%d', {PeriodsMeta.start}, {recursive_member_modifier})",
        'recursive_expr_formatted': f"STRFTIME('{format}', {PeriodsMeta.start}, {recursive_member_modifier})",
        'recursion_cond_expr': f"{PeriodsMeta.start} < '{end}'",
    }
    # yapf: enable

    return await AsyncSQLiteQueryExecutor().execute(
        main_query=SqlitePeriodsQueryAsync(format_params=format_params)
    )


class SqlitePeriodsQueryAsync(AsyncSqlQuery):

    def __init__(self, format_params: Mapping[str, str] = {}) -> None:
        super().__init__(
            query_file_path='',
            fields_mapping=PeriodsMeta.get_attrs(),
            fields=(PeriodsMeta.period, ),
            format_params=format_params,
            formatter=json_array_of_values,
        )

    async def _get_raw_query(self) -> str:
        return (
'''    WITH RECURSIVE periods({start}, {period}) AS (
        VALUES({anchor_expr}, {anchor_expr_formatted})
        UNION ALL
        SELECT {recursive_expr}, {recursive_expr_formatted}
        FROM periods
        WHERE {recursion_cond_expr}
    )
    SELECT * FROM periods'''
        )
