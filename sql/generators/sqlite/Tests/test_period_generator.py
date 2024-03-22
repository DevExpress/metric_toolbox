import pytest
import asyncio
from datetime import date
from dateutil.relativedelta import relativedelta
from toolbox.utils.converters import JSON_to_object
from toolbox.sql.generators.sqlite.periods_generator import (
    generate_periods,
    YMD,
    YW,
    YM,
    YQ,
    YH,
    Y,
    SELECTED_PERIOD,
)


@pytest.fixture
def loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# yapf: disable
@pytest.mark.parametrize(
    'start, end, format, res', [
        (
            '2019-01-01', '2021-01-01', YMD, [(date(2019, 1, 1) + relativedelta(days=x)).strftime(YMD) for x in range(731)]
        ),
        (
            '2019-01-01', '2021-01-01', YW, [(date(2018, 12, 31) + relativedelta(weeks=x)).strftime(YMD) for x in range(105)]
        ),
        (
            '2019-01-01', '2021-01-01', YM, [(date(2019, 1, 1) + relativedelta(months=x)).strftime(YM) for x in range(24)]
        ),
        (
            '2020-05-29', '2021-03-22', YM, [(date(2020, 5, 1) + relativedelta(months=x)).strftime(YM) for x in range(11)]
        ),
        (
            '2019-01-11', '2021-01-01', YQ, ['2019-1', '2019-2', '2019-3', '2019-4', '2020-1', '2020-2', '2020-3', '2020-4']
        ),
        (
            '2021-10-11', '2022-01-01', YQ, ['2021-4'],
        ),
        (
            '2021-08-13', '2021-10-11', YQ, ['2021-3', '2021-4'],
        ),
        (
            '2019-01-11', '2021-01-01', YH, ['2019-1', '2019-2',  '2020-1', '2020-2'],
        ),
        (
            '2021-10-23', '2022-01-01', YH, ['2021-2'],
        ),
        (
            '2019-01-11', '2022-01-01', Y, ['2019', '2020', '2021'],
        ),
        (
            '2019-01-11', '2022-01-01', SELECTED_PERIOD, [SELECTED_PERIOD],
        ),
    ]
)
def test_generate_periods(start, end, format, res, loop: asyncio.AbstractEventLoop):
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv('SQLITE_DATABASE', ':memory:')
        output = loop.run_until_complete(generate_periods(start, end, format))
        assert JSON_to_object.convert(output) == res
