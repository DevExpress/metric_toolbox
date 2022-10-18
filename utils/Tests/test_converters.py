import pytest
from typing import Callable, Any
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from toolbox.utils.Tests.data import df_data, TstClass

from toolbox.utils.converters import (
    JSON_to_DF,
    DF_to_JSON,
    JSON_to_object,
    Object_to_JSON,
    Objects_to_JSON,
)


@pytest.mark.parametrize(
    'convert, args, result',
    [
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
    ],
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
