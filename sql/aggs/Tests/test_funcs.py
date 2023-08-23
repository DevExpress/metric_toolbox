import pytest
from collections.abc import Callable
from toolbox.sql.aggs.functions import Func, SUM, COUNT, COUNT_DISTINCT


@pytest.mark.parametrize(
    'func, param, res', [
        (SUM, 'qwe', 'SUM(qwe)'),
        (COUNT, 'qwe', 'COUNT(qwe)'),
        (COUNT_DISTINCT, 'qwe', 'COUNT(DISTINCT qwe)'),
    ]
)
def test_str_param(func: Func, param: str, res: str):
    assert str(func(param)) == res


@pytest.mark.parametrize(
    'func, expr, res', [
        (SUM, 'expr1', 'expr1'),
        (COUNT, 'expr1', 'expr1'),
        (COUNT_DISTINCT, 'expr1', 'expr1'),
    ]
)
def test_str_expr(func: Func, expr: str, res: str):
    assert str(func('', expr)) == res


def test_eq():
    assert SUM('expr1') == SUM('expr1')
    assert str(SUM('', 'expr1')) == 'expr1'


@pytest.mark.parametrize(
    'func1, param1, func2, param2, res', [
        (
            SUM,
            'expr1',
            SUM,
            'expr2',
            '(SUM(expr1) + SUM(expr2))',
        ),
        (
            COUNT,
            'expr1',
            COUNT,
            'expr2',
            '(COUNT(expr1) + COUNT(expr2))',
        ),
        (
            COUNT_DISTINCT,
            'expr1',
            COUNT_DISTINCT,
            'expr2',
            '(COUNT(DISTINCT expr1) + COUNT(DISTINCT expr2))',
        ),
        (
            SUM,
            'expr1',
            lambda x: x,
            'expr2',
            '(SUM(expr1) + expr2)',
        ),
    ]
)
def test_add(
    func1: Callable,
    param1: str,
    func2: Callable,
    param2: str,
    res: str,
):
    assert str(func1(param1) + func2(param2)) == res


@pytest.mark.parametrize(
    'func1, param1, func2, param2, res', [
        (
            SUM,
            'expr1',
            SUM,
            'expr2',
            'SUM(expr1) * SUM(expr2)',
        ),
        (
            COUNT,
            'expr1',
            COUNT,
            'expr2',
            'COUNT(expr1) * COUNT(expr2)',
        ),
        (
            COUNT_DISTINCT,
            'expr1',
            COUNT_DISTINCT,
            'expr2',
            'COUNT(DISTINCT expr1) * COUNT(DISTINCT expr2)',
        ),
        (
            SUM,
            'expr1',
            lambda x: x,
            100,
            'SUM(expr1) * 100',
        ),
        (
            COUNT,
            'expr1',
            lambda x: x,
            'expr2',
            'COUNT(expr1) * expr2',
        ),
        (
            COUNT_DISTINCT,
            'expr1',
            lambda x: x,
            'expr2',
            'COUNT(DISTINCT expr1) * expr2',
        ),
    ]
)
def test_mul(
    func1: Callable,
    param1: str,
    func2: Callable,
    param2: str,
    res: str,
):
    assert str(func1(param1) * func2(param2)) == res


@pytest.mark.parametrize(
    'func1, param1, func2, param2, res', [
        (
            SUM,
            'expr1',
            SUM,
            'expr2',
            'IIF(SUM(expr2) = 0, 0, SUM(expr1) * 1.0 / SUM(expr2))',
        ),
        (
            COUNT,
            'expr1',
            COUNT,
            'expr2',
            'IIF(COUNT(expr2) = 0, 0, COUNT(expr1) * 1.0 / COUNT(expr2))',
        ),
        (
            COUNT_DISTINCT,
            'expr1',
            COUNT_DISTINCT,
            'expr2',
            'IIF(COUNT(DISTINCT expr2) = 0, 0, COUNT(DISTINCT expr1) * 1.0 / COUNT(DISTINCT expr2))',
        ),
        (
            COUNT_DISTINCT,
            'expr1',
            lambda x: x,
            'expr2',
            'IIF(expr2 = 0, 0, COUNT(DISTINCT expr1) * 1.0 / expr2)',
        ),
    ]
)
def test_div(func1: Func, param1: str, func2: Func, param2: str, res: str):
    assert str(func1(param1) / func2(param2)) == res


@pytest.mark.parametrize(
    'func1, param1, func2, param2, res', [
        (SUM, 'expr1', SUM, 'expr2', 'IIF(expr2 = 0, 0, expr1 * 1.0 / expr2)'),
        (
            COUNT, 'expr1', COUNT, 'expr2',
            'IIF(expr2 = 0, 0, expr1 * 1.0 / expr2)'
        ),
        (
            COUNT_DISTINCT, 'expr1', COUNT_DISTINCT, 'expr2',
            'IIF(expr2 = 0, 0, expr1 * 1.0 / expr2)'
        ),
    ]
)
def test_div_expr(
    func1: Func,
    param1: str,
    func2: Func,
    param2: str,
    res: str,
):
    assert str(func1('', param1) / func2('', param2)) == res


def test_div_zero():
    assert str(SUM('expr1') / SUM('expr2', iif_zero=1)) == 'IIF(SUM(expr2) = 0, 1, SUM(expr1) * 1.0 / SUM(expr2))'


@pytest.mark.parametrize(
    'func, param, wnd, res', [
        (SUM, 'expr1', 'qwe', 'SUM(expr1) OVER (qwe)'),
        (COUNT, 'expr1', 'qwe', 'COUNT(expr1) OVER (qwe)'),
    ]
)
def test_over(func: Func, param: str, wnd: str, res: str):
    assert func(param).over(wnd) == res


def test_count_distinct_over():
    with pytest.raises(SyntaxError):
        COUNT_DISTINCT('qwe').over('wnd')


@pytest.mark.parametrize(
    'func1, param1, func2, param2, wnd, res', [
        (
            SUM, 'expr1', SUM, 'expr2', 'qwe',
            'IIF(SUM(expr2) OVER (qwe) = 0, 0, SUM(expr1) OVER (qwe) * 1.0 / SUM(expr2) OVER (qwe))'
        ),
        (
            COUNT, 'expr1', COUNT, 'expr2', 'qwe',
            'IIF(COUNT(expr2) OVER (qwe) = 0, 0, COUNT(expr1) OVER (qwe) * 1.0 / COUNT(expr2) OVER (qwe))'
        ),
    ]
)
def test_div_over(
    func1: Func,
    param1: str,
    func2: Func,
    param2: str,
    wnd: str,
    res: str,
):
    assert (func1(param1) / func2(param2)).over(wnd) == res
