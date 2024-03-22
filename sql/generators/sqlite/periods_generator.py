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
    # %Y-%Q and %Y-%H are invalid, so we handle them.
    return '''[
    { "name": "Day",            "format": "%Y-%m-%d" },
    { "name": "Week-Year",      "format": "%Y-%W" },
    { "name": "Month-Year",     "format": "%Y-%m" },
    { "name": "Quarter-Year",   "format": "%Y-%Q" },
    { "name": "Half-Year",      "format": "%Y-%H" },
    { "name": "Year",           "format": "%Y" },
    { "name": "Selected Period","format": "FULL" }
]
'''


YMD = '%Y-%m-%d'
YW = '%Y-%W'
YM = '%Y-%m'
YQ = '%Y-%Q'
YH = '%Y-%H'
Y = '%Y'
SELECTED_PERIOD = 'FULL'

anchor_modifiers = {
    YMD: "'START OF DAY'",
    YW: "'WEEKDAY 0', '-6 DAYS'",
    YM: "'START OF MONTH'",
    YQ: "'START OF MONTH'",
    YH: "'START OF MONTH'",
    Y: "'START OF YEAR'",
}


# yapf: disable
def generate_group_by_period(format: str, field: str, modifier: str = '') -> str:
    if not modifier:
        modifier = anchor_modifiers.get(format, "'START OF DAY'")
    return {
        YW: f"STRFTIME('{YMD}', {field}, {modifier})",
        YM: f"STRFTIME('{YM}', {field}, {modifier})",
        YQ: f"(STRFTIME('{Y}', {field}, {modifier}) || -((STRFTIME('%m', {field}, {modifier}) + 2) / 3))",
        YH: f"(STRFTIME('{Y}', {field}, {modifier}) || -((STRFTIME('%m', {field}, {modifier}) + 7) / 7))",
        Y: f"STRFTIME('{Y}', {field}, {modifier})",
        SELECTED_PERIOD : f"'{SELECTED_PERIOD}'",
    }.get(format, f"STRFTIME('{YMD}', {field}, {modifier})")
# yapf: enable


async def generate_periods(
    start: str,
    end: str,
    format: str,
) -> str:
    if format == SELECTED_PERIOD:
        return f'["{SELECTED_PERIOD}"]'

    def negative(modifier: str) -> str:
        return "'-" + modifier[2:]

    recursive_member_modifier = {
        YMD: "'+1 DAYS'",
        YW: "'+7 DAYS'",
        YM: "'+1 MONTHS'",
        YQ: "'+1 MONTHS'",
        YH: "'+1 MONTHS'",
        Y: "'+1 YEAR'",
    }[format]

    anchor_modifier = anchor_modifiers[format]
    anchor_format = YMD
    if format == YW:
        anchor_format = format
    start = to_quoted_string(start)
    end = to_quoted_string(end)
    # yapf: disable
    format_params = {
        **PeriodsMeta.get_attrs(),
        'anchor_expr': generate_group_by_period(anchor_format, start, anchor_modifier),
        'anchor_expr_formatted':  generate_group_by_period(format, start, anchor_modifier),
        'recursive_expr': generate_group_by_period(anchor_format, PeriodsMeta.start, recursive_member_modifier),
        'recursive_expr_formatted': generate_group_by_period(format, PeriodsMeta.start, recursive_member_modifier),
        'recursion_cond_expr': f'{PeriodsMeta.start} < {generate_group_by_period(anchor_format, end, negative(recursive_member_modifier))}',
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
