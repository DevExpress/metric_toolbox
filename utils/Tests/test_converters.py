import pytest
from typing import Callable, Any
from pandas import DataFrame
from datetime import datetime
from pandas.testing import assert_frame_equal
from toolbox.utils.Tests.data import (
    df_data,
    ds_data,
    TstClass,
)

from toolbox.utils.converters import (
    JSON_to_DF,
    DF_to_JSON,
    JSON_to_object,
    Object_to_JSON,
    Objects_to_JSON,
    DateTimeColumnsConverter,
    DateTimeToSqlString,
)


@pytest.mark.parametrize(
    'convert, args, result', [
        (
            DF_to_JSON.convert,
            DataFrame(data=df_data),
            '[{"ds":"2022-04-17 04:00:00.000","y":1},{"ds":"2022-04-18 00:00:00.000","y":2},{"ds":"2022-04-19 01:00:00.000","y":3}]',
        ),
        (
            JSON_to_DF.convert,
            '[{"ds":"2022-04-17 04:00:00.000","y":1},{"ds":"2022-04-18 00:00:00.000","y":2},{"ds":"2022-04-19 01:00:00.000","y":3}]',
            DataFrame(data=df_data),
        ),
        (
            JSON_to_object.convert,
            '[1, 2, 3]',
            [1, 2, 3],
        ),
        (
            Object_to_JSON.convert,
            [1, 2, 3],
            '[1, 2, 3]',
        ),
        (
            Objects_to_JSON.convert,
            [
                TstClass('qwe'),
                TstClass('asd'),
                TstClass('3'),
            ],
            '["qwe", "asd", "3"]',
        ),
        (
            Objects_to_JSON.convert,
            [1, 2, 3],
            '[1, 2, 3]',
        ),
        (
            DateTimeToSqlString.convert,
            datetime(2022, 10, 13),
            '20221013',
        ),
        (
            DateTimeToSqlString.convert,
            datetime(2022, 9, 13),
            '20220913',
        ),
        (
            DateTimeToSqlString.convert,
            datetime(2022, 8, 9),
            '20220809',
        ),
        (
            DateTimeToSqlString.convert,
            datetime(2022, 11, 9),
            '20221109',
        ),
    ]
)
def test_converter(
    convert: Callable,
    args: Any,
    result: Any,
):
    if isinstance(result, DataFrame):
        assert_frame_equal(convert(args), result)
    else:
        assert convert(args) == result


def test_JSON_to_DF_raises_ValueError_if_json_is_empty():
    with pytest.raises(ValueError) as exec_info:
        JSON_to_DF.convert(json='')
    assert str(exec_info.value) == 'Empty response'


@pytest.mark.parametrize(
    'df,result,drop_utc', [
        (
            DataFrame(data=ds_data),
            'datetime64[ns, UTC]',
            False,
        ),
        (
            DataFrame(data=ds_data),
            'datetime64[ns]',
            True,
        ),
    ]
)
def test_DateTimeColumnsConverter(df: DataFrame, result: str, drop_utc: bool):
    DateTimeColumnsConverter.convert(
        df=df,
        cols=['ds'],
        drop_utc=drop_utc,
    )
    assert str(df['ds'].dtype) == result
