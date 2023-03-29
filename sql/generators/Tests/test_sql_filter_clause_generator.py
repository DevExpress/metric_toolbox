import pytest
from typing import List
from typing import Callable, Any
from toolbox.utils.converters import to_quoted_string
import toolbox.sql.generators.filter_clause_generator as SqlFilterClauseGenerator


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
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE col IN ('p1','p2')",
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
            [1, 2],
            'AND',
            str,
            "AND col IN (1,2)",
        ),
        (
            'col',
            [1],
            'AND',
            str,
            "AND col IN (1)",
        ),
    ]
)
def test_generate_in_filter(
    col: str,
    values: List[str],
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
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE (col IS NULL OR col NOT IN ('p1','p2'))",
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
    values: List[str],
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
            ['p1', 'p2'],
            'WHERE',
            "WHERE (col LIKE '%p1%' OR col LIKE '%p2%')",
        ),
        (
            'col',
            ['p1'],
            'AND',
            "AND (col LIKE '%p1%')",
        ),
    ]
)
def test_generate_like_filter(
    col: str,
    values: List[str],
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
            ['p1', 'p2'],
            'WHERE',
            "WHERE (col IS NULL OR NOT (col LIKE '%p1%' OR col LIKE '%p2%'))",
        ),
        (
            'col',
            ['p1'],
            'AND',
            "AND (col IS NULL OR NOT (col LIKE '%p1%'))",
        ),
    ]
)
def test_generate_not_like_filter(
    col: str,
    values: List[str],
    prefix: str,
    output: str,
):
    assert SqlFilterClauseGenerator.generate_not_like_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
    ) == output


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
            ['p1', 'p2'],
            'WHERE',
            to_quoted_string,
            "WHERE col BETWEEN 'p1' AND 'p2'",
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
    values: List[str],
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
    ]
)
def test_generate_not_between_filter(
    col: str,
    values: List[str],
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
