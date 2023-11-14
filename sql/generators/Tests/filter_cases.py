from toolbox.sql.generators.Tests.mocks import (
    MockFilterParametersNode,
    MockFilterParameterNode,
)
from toolbox.utils.converters import to_quoted_string
from toolbox.sql.generators import NULL_FILTER_VALUE


# yapf: disable
def single_in_filter_cases(convert, prefix='WHERE'):
    convert = convert or to_quoted_string
    return [
        (
            None,
            '',
        ),
        (
            MockFilterParametersNode(include=True, values=[]),
            '',
        ),
        (
            MockFilterParametersNode(include=False, values=[]),
            prefix + ' {field} IS NULL',
        ),
        (
            MockFilterParametersNode(include=True, values=[123]),
            prefix + ' {field} IN ' + f'({convert(123)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[123]),
            prefix + ' ({field} IS NULL OR {field} NOT IN ' + f'({convert(123)}))',
        ),
        (
            MockFilterParametersNode(include=True, values=[1, 2]),
            prefix + ' {field} IN ' + f'({convert(1)},{convert(2)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[1, 2]),
            prefix + ' ({field} IS NULL OR {field} NOT IN ' + f'({convert(1)},{convert(2)}))',
        ),
        (
            MockFilterParametersNode(include=True, values=[1, 2, NULL_FILTER_VALUE]),
            prefix + ' ({field} IS NULL OR {field} IN ' + f'({convert(1)},{convert(2)}))',
        ),
        (
            MockFilterParametersNode(include=True, values=[NULL_FILTER_VALUE]),
            prefix + ' {field} IS NULL',
        ),
        (
            MockFilterParametersNode(include=False, values=[NULL_FILTER_VALUE]),
            prefix + ' {field} IS NOT NULL',
        ),
        (
            MockFilterParametersNode(include=False, values=[1, 2, NULL_FILTER_VALUE]),
            prefix + ' ({field} IS NOT NULL AND {field} NOT IN ' + f'({convert(1)},{convert(2)}))',
        ),
    ]


def double_in_filter_cases(convert, prefix='WHERE'):
    convert = convert or to_quoted_string
    return [
        (
            None,
            None,
            '',
        ),
        (
            MockFilterParametersNode(include=True, values=[]),
            None,
            '',
        ),
        (
            MockFilterParametersNode(include=True, values=[]),
            MockFilterParametersNode(include=True, values=[]),
            '',
        ),
        (
            MockFilterParametersNode(include=False, values=[]),
            MockFilterParametersNode(include=True, values=[]),
            prefix + ' {field1} IS NULL',
        ),
        (
            MockFilterParametersNode(include=True, values=[]),
            MockFilterParametersNode(include=False, values=[]),
            prefix + ' {field2} IS NULL',
        ),
        (
            MockFilterParametersNode(include=False, values=[]),
            MockFilterParametersNode(include=False, values=[]),
            prefix + ' {field1} IS NULL AND {field2} IS NULL',
        ),
        (
            MockFilterParametersNode(include=True, values=[123]),
            MockFilterParametersNode(include=True, values=[]),
            prefix + ' {field1} IN ' + f'({convert(123)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[123]),
            MockFilterParametersNode(include=True, values=[]),
            prefix + ' ({field1} IS NULL OR {field1} NOT IN ' + f'({convert(123)}))',
        ),
        (
            MockFilterParametersNode(include=True, values=[123]),
            MockFilterParametersNode(include=False, values=[]),
            prefix + ' {field1} IN ' + f'({convert(123)})' + ' AND {field2} IS NULL',
        ),
        (
            MockFilterParametersNode(include=False, values=[123]),
            MockFilterParametersNode(include=False, values=[]),
            prefix + ' ({field1} IS NULL OR {field1} NOT IN ' + f'({convert(123)}))' + ' AND {field2} IS NULL',
        ),
        (
            MockFilterParametersNode(include=True, values=[]),
            MockFilterParametersNode(include=True, values=[123]),
            prefix + ' {field2} IN ' + f'({convert(123)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[]),
            MockFilterParametersNode(include=True, values=[123]),
            prefix + ' {field1} IS NULL AND {field2} IN ' + f'({convert(123)})',
        ),
        (
            MockFilterParametersNode(include=True, values=[]),
            MockFilterParametersNode(include=False, values=[123]),
            prefix + ' ({field2} IS NULL OR {field2} NOT IN ' + f'({convert(123)}))',
        ),
        (
            MockFilterParametersNode(include=False, values=[]),
            MockFilterParametersNode(include=False, values=[123]),
            prefix + ' {field1} IS NULL AND ({field2} IS NULL OR {field2} NOT IN ' + f'({convert(123)}))',
        ),
        (
            MockFilterParametersNode(include=True, values=[1]),
            MockFilterParametersNode(include=True, values=[2]),
            prefix + ' {field1} IN ' + f'({convert(1)})' + ' AND {field2} IN ' + f'({convert(2)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[1]),
            MockFilterParametersNode(include=True, values=[2]),
            prefix + ' ({field1} IS NULL OR {field1} NOT IN ' + f'({convert(1)}))' + ' AND {field2} IN ' + f'({convert(2)})',
        ),
        (
            MockFilterParametersNode(include=True, values=[1]),
            MockFilterParametersNode(include=False, values=[2]),
            prefix + ' {field1} IN ' + f'({convert(1)})' + ' AND ({field2} IS NULL OR {field2} NOT IN ' + f'({convert(2)}))',
        ),
        (
            MockFilterParametersNode(include=False, values=[1]),
            MockFilterParametersNode(include=False, values=[2]),
            prefix + ' ({field1} IS NULL OR {field1} NOT IN ' + f'({convert(1)}))' + ' AND ({field2} IS NULL OR {field2} NOT IN ' + f'({convert(2)}))',
        ),
        (
            MockFilterParametersNode(include=True, values=[1, 2]),
            MockFilterParametersNode(include=True, values=[3, 4]),
            prefix + ' {field1} IN ' + f'({convert(1)},{convert(2)})' + ' AND {field2} IN ' + f'({convert(3)},{convert(4)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[1, 2]),
            MockFilterParametersNode(include=True, values=[3, 4]),
            prefix + ' ({field1} IS NULL OR {field1} NOT IN ' + f'({convert(1)},{convert(2)}))' + ' AND {field2} IN ' + f'({convert(3)},{convert(4)})',
        ),
        (
            MockFilterParametersNode(include=True, values=[1, 2]),
            MockFilterParametersNode(include=False, values=[3, 4]),
            prefix + ' {field1} IN ' + f'({convert(1)},{convert(2)})' + ' AND ({field2} IS NULL OR {field2} NOT IN ' + f'({convert(3)},{convert(4)}))'
        ),
        (
            MockFilterParametersNode(include=False, values=[1, 2]),
            MockFilterParametersNode(include=False, values=[3, 4]),
            prefix + ' ({field1} IS NULL OR {field1} NOT IN ' + f'({convert(1)},{convert(2)}))' + ' AND ({field2} IS NULL OR {field2} NOT IN ' + f'({convert(3)},{convert(4)}))',
        ),
        (
            MockFilterParametersNode(include=True, values=[1, 2]),
            MockFilterParametersNode(include=True, values=[3]),
            prefix + ' {field1} IN ' + f'({convert(1)},{convert(2)})' + ' AND {field2} IN ' + f'({convert(3)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[1, 2]),
            MockFilterParametersNode(include=True, values=[3]),
            prefix + ' ({field1} IS NULL OR {field1} NOT IN ' + f'({convert(1)},{convert(2)}))' + ' AND {field2} IN ' + f'({convert(3)})',
        ),
        (
            MockFilterParametersNode(include=True, values=[1, 2]),
            MockFilterParametersNode(include=False, values=[3]),
            prefix + ' {field1} IN ' + f'({convert(1)},{convert(2)})' + ' AND ({field2} IS NULL OR {field2} NOT IN ' + f'({convert(3)}))',
        ),
        (
            MockFilterParametersNode(include=False, values=[1, 2]),
            MockFilterParametersNode(include=False, values=[3]),
            prefix + ' ({field1} IS NULL OR {field1} NOT IN ' + f'({convert(1)},{convert(2)}))' + ' AND ({field2} IS NULL OR {field2} NOT IN ' + f'({convert(3)}))',
        ),
    ]


def single_like_filter_cases(convert=None, prefix=None):
    return [
        (
            None,
            '',
        ),
        (
            MockFilterParametersNode(include=True, values=[]),
            '',
        ),
        (
            MockFilterParametersNode(include=False, values=[]),
            'AND {field} IS NULL',
        ),
        (
            MockFilterParametersNode(include=True, values=['p1', 'p2']),
            "AND ({field} LIKE '%p1%' OR {field} LIKE '%p2%')"
        ),
        (
            MockFilterParametersNode(include=True, values=['p1']),
            "AND ({field} LIKE '%p1%')"
        ),
        (
            MockFilterParametersNode(include=False, values=['p1', 'p2']),
            "AND ({field} IS NULL OR NOT ({field} LIKE '%p1%' OR {field} LIKE '%p2%'))"
        ),
        (
            MockFilterParametersNode(include=False, values=['p1']),
            "AND ({field} IS NULL OR NOT ({field} LIKE '%p1%'))"
        ),
        (
            MockFilterParametersNode(include=True, values=['p1', 'p2', NULL_FILTER_VALUE]),
            "AND ({field} IS NULL OR ({field} LIKE '%p1%' OR {field} LIKE '%p2%'))"
        ),
        (
            MockFilterParametersNode(include=True, values=[NULL_FILTER_VALUE]),
            "AND {field} IS NULL"
        ),
        (
            MockFilterParametersNode(include=False, values=[NULL_FILTER_VALUE]),
            "AND {field} IS NOT NULL"
        ),
        (
            MockFilterParametersNode(include=False, values=['p1', 'p2', NULL_FILTER_VALUE]),
            "AND ({field} IS NOT NULL AND NOT ({field} LIKE '%p1%' OR {field} LIKE '%p2%'))",
        ),
    ]


def between_filter_cases(convert, prefix='WHERE'):
    convert = convert or to_quoted_string
    return [
        (
            None,
            '',
        ),
        (
            MockFilterParametersNode(include=True, values=[]),
            '',
        ),
        (
            MockFilterParametersNode(include=True, values=[NULL_FILTER_VALUE]),
            prefix + ' {field} IS NULL',
        ),
        (
            MockFilterParametersNode(include=False, values=[]),
            prefix + ' {field} IS NULL',
        ),
        (
            MockFilterParametersNode(include=False, values=[NULL_FILTER_VALUE]),
            prefix + ' {field} IS NOT NULL',
        ),
        (
            MockFilterParametersNode(include=True, values=[1, 2]),
            prefix + ' {field} BETWEEN ' + f'{convert(1)} AND {convert(2)}',
        ),
         (
            MockFilterParametersNode(include=True, values=[1, 2, NULL_FILTER_VALUE]),
            prefix + ' ({field} IS NULL OR {field} BETWEEN ' + f'{convert(1)} AND {convert(2)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[1, 2]),
            prefix + ' ({field} IS NULL OR {field} NOT BETWEEN ' + f'{convert(1)} AND {convert(2)})',
        ),
        (
            MockFilterParametersNode(include=False, values=[1, 2, NULL_FILTER_VALUE]),
            prefix + ' ({field} IS NOT NULL AND {field} NOT BETWEEN ' + f'{convert(1)} AND {convert(2)})',
        ),
    ]


def equals_filter_cases(convert, prefix='WHERE'):
    convert = convert or to_quoted_string
    return [
        (
            MockFilterParameterNode(include=True, value=1),
            prefix + ' {field} = ' + f'{convert(1)}'
        ),
        (
            MockFilterParameterNode(include=False, value=1),
            prefix + ' ({field} IS NULL OR {field} != ' + f'{convert(1)})'
        ),
    ]

def less_filter_cases(convert, prefix='WHERE'):
    convert = convert or to_quoted_string
    return [
        (
            MockFilterParameterNode(include=True, value=1),
            prefix + ' ({field} IS NOT NULL AND {field} < ' + f'{convert(1)})'
        ),
        (
            MockFilterParameterNode(include=False, value=1),
            prefix + ' ({field} IS NOT NULL AND {field} >= ' + f'{convert(1)})'
        ),
        (
            MockFilterParameterNode(include=True, value=NULL_FILTER_VALUE),
            prefix + ' {field} IS NULL',
        ),
        (
            MockFilterParameterNode(include=False, value=NULL_FILTER_VALUE),
            prefix + ' {field} IS NOT NULL',
        ),
    ]

def less_equals_filter_cases(convert, prefix='WHERE'):
    convert = convert or to_quoted_string
    return [
        (
            MockFilterParameterNode(include=True, value=1),
            prefix + ' ({field} IS NOT NULL AND {field} <= ' + f'{convert(1)})'
        ),
        (
            MockFilterParameterNode(include=False, value=1),
            prefix + ' ({field} IS NOT NULL AND {field} > ' + f'{convert(1)})'
        ),
        (
            MockFilterParameterNode(include=True, value=NULL_FILTER_VALUE),
            prefix + ' {field} IS NULL',
        ),
        (
            MockFilterParameterNode(include=False, value=NULL_FILTER_VALUE),
            prefix + ' {field} IS NOT NULL',
        ),
    ]


def right_half_open_interval_filter_cases(convert, prefix='WHERE'):
    convert = convert or to_quoted_string
    return [
        (
            MockFilterParameterNode(include=True, values=[1, 2]),
            prefix + ' ({field} IS NOT NULL AND' + f' {convert(1)} ' +'<= {field} AND {field} <' + f' {convert(2)})'
        ),
        (
            MockFilterParameterNode(include=False, values=[1, 2]),
            prefix + ' ({field} IS NOT NULL AND '+ f'{convert(1)} ' + '> {field} AND {field} >=' + f' {convert(2)})'
        ),
    ]
