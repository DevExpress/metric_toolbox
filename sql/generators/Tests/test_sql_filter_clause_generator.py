import pytest
from typing import Callable, Any
from toolbox.utils.converters import to_quoted_string
import toolbox.sql.generators.filter_clause_generator as SqlFilterClauseGenerator
from toolbox.sql.generators import NULL_FILTER_VALUE


@pytest.mark.parametrize(
    'col, values, prefix, converter, output', [
        (
            'col',
            [],
            'WHERE',
            str,
            '',
        ),
        (
            'col',
            [NULL_FILTER_VALUE],
            'WHERE',
            str,
            'WHERE col IS NULL',
        ),
        (
            'col',
            ['p1'],
            'WHERE',
            to_quoted_string,
            "WHERE col IN ('p1')",
        ),
        (
            'col',
            ['p1', NULL_FILTER_VALUE],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR col IN ('p1'))",
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE col IN ('p1','p2')",
        ),
        (
            'col',
            ['p1', 'p2', NULL_FILTER_VALUE],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR col IN ('p1','p2'))",
        ),
        (
            'col',
            [1],
            'AND',
            str,
            "AND col IN (1)",
        ),
        (
            'col',
            [1, 2],
            'AND',
            str,
            "AND col IN (1,2)",
        ),
    ]
)
def test_generate_in_filter(
    col: str,
    values: list[str],
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_in_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
        values_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, values, prefix, converter, output', [
        (
            'col',
            [],
            'WHERE',
            str,
            'WHERE col IS NULL',
        ),
        (
            'col',
            [NULL_FILTER_VALUE],
            'WHERE',
            str,
            'WHERE col IS NOT NULL',
        ),
        (
            'col',
            ['p1'],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR col NOT IN ('p1'))",
        ),
        (
            'col',
            ['p1', NULL_FILTER_VALUE],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND col NOT IN ('p1'))",
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR col NOT IN ('p1','p2'))",
        ),
        (
            'col',
            ['p1', 'p2', NULL_FILTER_VALUE],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND col NOT IN ('p1','p2'))",
        ),
        (
            'col',
            [1, 2],
            'AND',
            str,
            "AND (col IS NULL OR col NOT IN (1,2))",
        ),
        (
            'col',
            [1],
            'AND',
            str,
            "AND (col IS NULL OR col NOT IN (1))",
        ),
    ]
)
def test_generate_not_in_filter(
    col: str,
    values: list[str],
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_not_in_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
        values_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, values, prefix, output', [
        (
            'col',
            [],
            'WHERE',
            '',
        ),
        (
            'col',
            [NULL_FILTER_VALUE],
            'WHERE',
            'WHERE col IS NULL',
        ),
        (
            'col',
            ['p1'],
            'AND',
            "AND (col LIKE '%p1%')",
        ),
        (
            'col',
            ['p1', NULL_FILTER_VALUE],
            'AND',
            "AND (col IS NULL OR (col LIKE '%p1%'))",
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE',
            "WHERE (col LIKE '%p1%' OR col LIKE '%p2%')",
        ),
        (
            'col',
            ['p1', 'p2', NULL_FILTER_VALUE],
            'WHERE',
            "WHERE (col IS NULL OR (col LIKE '%p1%' OR col LIKE '%p2%'))",
        ),
    ]
)
def test_generate_like_filter(
    col: str,
    values: list[str],
    prefix: str,
    output: str,
):
    assert SqlFilterClauseGenerator.generate_like_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
    ) == output


@pytest.mark.parametrize(
    'col, values, prefix, output', [
        (
            'col',
            [],
            'WHERE',
            'WHERE col IS NULL',
        ),
        (
            'col',
            [NULL_FILTER_VALUE],
            'WHERE',
            'WHERE col IS NOT NULL',
        ),
        (
            'col',
            ['p1'],
            'AND',
            "AND (col IS NULL OR NOT (col LIKE '%p1%'))",
        ),
        (
            'col',
            ['p1', NULL_FILTER_VALUE],
            'AND',
            "AND (col IS NOT NULL AND NOT (col LIKE '%p1%'))",
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE',
            "WHERE (col IS NULL OR NOT (col LIKE '%p1%' OR col LIKE '%p2%'))",
        ),
        (
            'col',
            ['p1', 'p2', NULL_FILTER_VALUE],
            'WHERE',
            "WHERE (col IS NOT NULL AND NOT (col LIKE '%p1%' OR col LIKE '%p2%'))",
        ),
    ]
)
def test_generate_not_like_filter(
    col: str,
    values: list[str],
    prefix: str,
    output: str,
):
    assert SqlFilterClauseGenerator.generate_not_like_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
    ) == output


def test_generate_is_null_filter():
    assert SqlFilterClauseGenerator.generate_is_null_filter(
        col='col',
        filter_prefix='WHERE',
    ) == 'WHERE col IS NULL'


def test_generate_is_not_null_filter():
    assert SqlFilterClauseGenerator.generate_is_not_null_filter(
        col='col',
        filter_prefix='WHERE',
    ) == 'WHERE col IS NOT NULL'


@pytest.mark.parametrize(
    'col, values, prefix, converter, output', [
        (
            'col',
            [],
            'WHERE',
            str,
            '',
        ),
        (
            'col',
            [NULL_FILTER_VALUE],
            'WHERE',
            str,
            'WHERE col IS NULL',
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE col BETWEEN 'p1' AND 'p2'",
        ),
        (
            'col',
            ['p1', 'p2', NULL_FILTER_VALUE],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR col BETWEEN 'p1' AND 'p2')",
        ),
        (
            'col',
            [1, 2],
            'AND',
            str,
            'AND col BETWEEN 1 AND 2',
        ),
    ]
)
def test_generate_between_filter(
    col: str,
    values: list[str],
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_between_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
        values_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, values, prefix, converter, output', [
        (
            'col',
            [],
            'WHERE',
            str,
            'WHERE col IS NULL',
        ),
        (
            'col',
            [NULL_FILTER_VALUE],
            'WHERE',
            str,
            'WHERE col IS NOT NULL',
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR col NOT BETWEEN 'p1' AND 'p2')",
        ),
        (
            'col',
            [1, 2],
            'AND',
            str,
            'AND (col IS NULL OR col NOT BETWEEN 1 AND 2)',
        ),
        (
            'col',
            [1, 2, NULL_FILTER_VALUE],
            'AND',
            str,
            'AND (col IS NOT NULL AND col NOT BETWEEN 1 AND 2)',
        ),
    ]
)
def test_generate_not_between_filter(
    col: str,
    values: list[str],
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_not_between_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
        values_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, values, prefix, converter, output', [
        (
            'col',
            [],
            'WHERE',
            str,
            '',
        ),
        (
            'col',
            [NULL_FILTER_VALUE],
            'WHERE',
            str,
            'WHERE col IS NULL',
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND 'p1' <= col AND col < 'p2')",
        ),
        (
            'col',
            ['p1', 'p2', NULL_FILTER_VALUE],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR ('p1' <= col AND col < 'p2'))",
        ),
        (
            'col',
            [1, 2],
            'AND',
            str,
            'AND (col IS NOT NULL AND 1 <= col AND col < 2)',
        ),
    ]
)
def test_generate_right_half_open_interval_filter(
    col: str,
    values: list[str],
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_right_halfopen_interval_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
        values_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, values, prefix, converter, output', [
        (
            'col',
            [],
            'WHERE',
            str,
            'WHERE col IS NOT NULL',
        ),
        (
            'col',
            [NULL_FILTER_VALUE],
            'WHERE',
            str,
            'WHERE col IS NOT NULL',
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND 'p1' > col AND col >= 'p2')",
        ),
        (
            'col',
            ['p1', 'p2', NULL_FILTER_VALUE],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND 'p1' > col AND col >= 'p2')",
        ),
        (
            'col',
            [1, 2],
            'AND',
            str,
            'AND (col IS NOT NULL AND 1 > col AND col >= 2)',
        ),
    ]
)
def test_generate_not_right_halfopen_interval_filter(
    col: str,
    values: list[str],
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_not_right_halfopen_interval_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
        values_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, value, prefix, converter, output', (
        (
            'col',
            0,
            'AND',
            lambda x: x,
            'AND col = 0',
        ),
        (
            'col',
            NULL_FILTER_VALUE,
            'AND',
            lambda x: x,
            'AND col IS NULL',
        ),
        (
            'col',
            1,
            'WHERE',
            to_quoted_string,
            "WHERE col = '1'",
        ),
    )
)
def test_equals_filter(
    col: str,
    value: Any,
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_equals_filter(
        col=col,
        value=value,
        filter_prefix=prefix,
        value_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, value, prefix, converter, output', (
        (
            'col',
            0,
            'AND',
            lambda x: x,
            'AND (col IS NULL OR col != 0)',
        ),
        (
            'col',
            NULL_FILTER_VALUE,
            'AND',
            lambda x: x,
            'AND col IS NOT NULL',
        ),
        (
            'col',
            1,
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR col != '1')",
        ),
    )
)
def test_not_equals_filter(
    col: str,
    value: Any,
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_not_equals_filter(
        col=col,
        value=value,
        filter_prefix=prefix,
        value_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, value, prefix, converter, output', (
        (
            'col',
            0,
            'AND',
            lambda x: x,
            'AND (col IS NOT NULL AND col <= 0)',
        ),
        (
            'col',
            NULL_FILTER_VALUE,
            'AND',
            lambda x: x,
            'AND col IS NULL',
        ),
        (
            'col',
            1,
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND col <= '1')",
        ),
    )
)
def test_less_equals_filter(
    col: str,
    value: Any,
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_less_equals_filter(
        col=col,
        value=value,
        filter_prefix=prefix,
        value_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, value, prefix, converter, output', (
        (
            'col',
            0,
            'AND',
            lambda x: x,
            'AND (col IS NOT NULL AND col > 0)',
        ),
        (
            'col',
            NULL_FILTER_VALUE,
            'AND',
            lambda x: x,
            'AND col IS NOT NULL',
        ),
        (
            'col',
            1,
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND col > '1')",
        ),
    )
)
def test_not_less_equals_filter(
    col: str,
    value: Any,
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_not_less_equals_filter(
        col=col,
        value=value,
        filter_prefix=prefix,
        value_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, value, prefix, converter, output', (
        (
            'col',
            0,
            'AND',
            lambda x: x,
            'AND (col IS NOT NULL AND col < 0)',
        ),
        (
            'col',
            NULL_FILTER_VALUE,
            'AND',
            lambda x: x,
            'AND col IS NULL',
        ),
        (
            'col',
            1,
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND col < '1')",
        ),
    )
)
def test_less_filter(
    col: str,
    value: Any,
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_less_filter(
        col=col,
        value=value,
        filter_prefix=prefix,
        value_converter=converter,
    ) == output


@pytest.mark.parametrize(
    'col, value, prefix, converter, output', (
        (
            'col',
            0,
            'AND',
            lambda x: x,
            'AND (col IS NOT NULL AND col >= 0)',
        ),
        (
            'col',
            NULL_FILTER_VALUE,
            'AND',
            lambda x: x,
            'AND col IS NOT NULL',
        ),
        (
            'col',
            1,
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NOT NULL AND col >= '1')",
        ),
    )
)
def test_not_less_filter(
    col: str,
    value: Any,
    prefix: str,
    converter: Callable[[Any], str],
    output: str,
):
    assert SqlFilterClauseGenerator.generate_not_less_filter(
        col=col,
        value=value,
        filter_prefix=prefix,
        value_converter=converter,
    ) == output
