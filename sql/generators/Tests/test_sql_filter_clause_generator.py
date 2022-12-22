import pytest
from typing import Callable, Any
from toolbox.sql.generators.filter_clause_generator import SqlFilterClauseGenerator


@pytest.mark.parametrize(
    'col, values, prefix, converter, output', [
        (
            'col',
            [],
            'WHERE ',
            str,
            '',
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE ',
            lambda val: f"'{val}'",
            "WHERE col IN ('p1','p2')",
        ),
        (
            'col',
            ['p1'],
            'WHERE ',
            lambda val: f"'{val}'",
            "WHERE col IN ('p1')",
        ),
        (
            'col',
            [1, 2],
            'AND ',
            str,
            "AND col IN (1,2)",
        ),
        (
            'col',
            [1],
            'AND ',
            str,
            "AND col IN (1)",
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
    assert SqlFilterClauseGenerator().generate_in_filter(
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
            'WHERE ',
            str,
            '',
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE ',
            lambda val: f"'{val}'",
            "WHERE (col IS NULL OR col NOT IN ('p1','p2'))",
        ),
        (
            'col',
            ['p1'],
            'WHERE ',
            lambda val: f"'{val}'",
            "WHERE (col IS NULL OR col NOT IN ('p1'))",
        ),
        (
            'col',
            [1, 2],
            'AND ',
            str,
            "AND (col IS NULL OR col NOT IN (1,2))",
        ),
        (
            'col',
            [1],
            'AND ',
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
    assert SqlFilterClauseGenerator().generate_not_in_filter(
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
            'WHERE ',
            '',
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE ',
            "WHERE (col LIKE '%p1%' OR col LIKE '%p2%')",
        ),
        (
            'col',
            ['p1'],
            'AND ',
            "AND (col LIKE '%p1%')",
        ),
    ]
)
def test_generate_like_filter(
    col: str,
    values: list[str],
    prefix: str,
    output: str,
):
    assert SqlFilterClauseGenerator().generate_like_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
    ) == output


@pytest.mark.parametrize(
    'col, values, prefix, output', [
        (
            'col',
            [],
            'WHERE ',
            '',
        ),
        (
            'col',
            ['p1', 'p2'],
            'WHERE ',
            "WHERE (col IS NULL OR NOT (col LIKE '%p1%' OR col LIKE '%p2%'))",
        ),
        (
            'col',
            ['p1'],
            'AND ',
            "AND (col IS NULL OR NOT (col LIKE '%p1%'))",
        ),
    ]
)
def test_generate_not_like_filter(
    col: str,
    values: list[str],
    prefix: str,
    output: str,
):
    assert SqlFilterClauseGenerator().generate_not_like_filter(
        col=col,
        values=values,
        filter_prefix=prefix,
    ) == output
