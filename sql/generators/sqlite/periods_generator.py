from collections.abc import Mapping
from toolbox.sql.meta_data import MetaData
from toolbox.sql_async import AsyncSQLiteQueryExecutor
from toolbox.utils import json_array_of_values
from toolbox.utils.converters import to_quoted_string
from toolbox.sql_async.sql_query import AsyncSqlQuery


class PeriodsMeta(MetaData):
    start = 'start'
    period = 'period'


async def get_group_by_periods_json():
    # format should contain a valid strftime string.
    # https://sqlite.org/lang_datefunc.html
    # %Y-%Q is invalid, so we handle it.
    return '''[
    { "name": "Day",            "format": "%Y-%m-%d" },
    { "name": "Week-Year",      "format": "%Y-%W" },
    { "name": "Month-Year",     "format": "%Y-%m" },
    { "name": "Quarter-Year",   "format": "%Y-%Q" },
    { "name": "Year",           "format": "%Y" }
]
'''


def generate_group_by_period(format: str, field: str, modifier: str = '') -> str:
    return {
        '%Y-%W': f"STRFTIME('%Y-%m-%d', {field}, {to_quoted_string(modifier) if modifier else to_quoted_string('WEEKDAY 0') + ',' + to_quoted_string('-6 DAYS')})",
        '%Y-%m': f"STRFTIME('%Y-%m', {field}, '{modifier or 'START OF MONTH'}')",
        '%Y-%Q': f"(STRFTIME('%Y', {field}) || -((STRFTIME('%m', {field}, '{modifier or 'START OF MONTH'}') + 2) / 3))",
        '%Y': f"STRFTIME('%Y', {field}, '{modifier or 'START OF YEAR'}')",
    }.get(format, f"STRFTIME('%Y-%m-%d', {field}, '{modifier or 'START OF DAY'}')")


async def generate_periods(
    start: str,
    end: str,
    format: str,
) -> str:
    recursive_member_modifier = {
        '%Y-%m-%d': "+1 DAYS",
        '%Y-%W': "+7 DAYS",
        '%Y-%m': "+1 MONTHS",
        '%Y-%Q': "+1 MONTHS",
        '%Y': "+1 YEAR",
    }[format]

    anchor_format = '%Y-%m-%d'
    if format == '%Y-%W':
        anchor_format = format
    start = to_quoted_string(start)
    # yapf: disable
    format_params = {
        **PeriodsMeta.get_attrs(),
        'anchor_expr': generate_group_by_period(anchor_format, start),
        'anchor_expr_formatted':  generate_group_by_period(format, start), 
        'recursive_expr': generate_group_by_period(anchor_format, PeriodsMeta.start, recursive_member_modifier),
        'recursive_expr_formatted': generate_group_by_period(format, PeriodsMeta.start, recursive_member_modifier),
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
    SELECT DISTINCT {period} FROM periods'''
        )
