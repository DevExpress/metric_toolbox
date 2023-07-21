import pytest
from toolbox.sql.aggs.functions import SUM
from toolbox.sql.aggs.metrics import Metric


# yapf: disable
def test_str():
    assert str(Metric('', '', '', 'expr1')) == 'expr1'

def test_eq():
    assert Metric('', '', '', SUM('expr1')) == Metric('', '', '', SUM('expr1'))

def test_add():
    assert Metric('', '', '', SUM('expr1')) + Metric('', '', '', SUM('expr2')) == Metric('', '', '', SUM('expr1') + SUM('expr2'))

def test_mul():
    assert Metric('', '', '', SUM('expr1')) * Metric('', '', '', SUM('expr2')) == Metric('', '', '', SUM('expr1') * SUM('expr2'))

def test_div():
    assert Metric('', '', '', SUM('expr1')) / Metric('', '', '', SUM('expr2')) == Metric('', '', '', SUM('expr1') / SUM('expr2'))

@pytest.mark.parametrize(
    'func, window, res', [
        (
            SUM('qwe'),
            'wnd',
            'SUM(qwe) OVER (wnd)',
        ),
        (
            SUM('qwe') / SUM('asd'),
            'wnd',
            'IIF(SUM(asd) OVER (wnd) = 0, 0, SUM(qwe) OVER (wnd) * 1.0 / SUM(asd) OVER (wnd))',
        ),
        (
            (SUM('qwe') / SUM('asd')) / SUM('zxc'),
            'wnd',
            'IIF(SUM(zxc) OVER (wnd) = 0, 0, IIF(SUM(asd) OVER (wnd) = 0, 0, SUM(qwe) OVER (wnd) * 1.0 / SUM(asd) OVER (wnd)) * 1.0 / SUM(zxc) OVER (wnd))',
        ),
        (
            SUM('qwe') / (SUM('asd') + SUM('zxc')),
            'wnd',
            'IIF((SUM(asd) OVER (wnd) + SUM(zxc) OVER (wnd)) = 0, 0, SUM(qwe) OVER (wnd) * 1.0 / (SUM(asd) OVER (wnd) + SUM(zxc) OVER (wnd)))',
        ),
        (
            (SUM('qwe') + SUM('asd')) / (SUM('zxc') + SUM('wer')),
            'wnd',
            'IIF((SUM(zxc) OVER (wnd) + SUM(wer) OVER (wnd)) = 0, 0, (SUM(qwe) OVER (wnd) + SUM(asd) OVER (wnd)) * 1.0 / (SUM(zxc) OVER (wnd) + SUM(wer) OVER (wnd)))',
        ),
    ]
)
def test_over_simple(func, window, res):
    assert Metric('tst', '', '', func).get_over(window) == res

def test_over():
    m1 = Metric('', '', '', SUM('qwe'))
    m2 = Metric('', '', '', SUM('asd'))
    m3 = Metric('', '', '', SUM('zxc'))
    metric = Metric.from_metric('tst', '', '', (m1 + m2) / m3)
    res = 'IIF(SUM(zxc) OVER (wnd) = 0, 0, (SUM(qwe) OVER (wnd) + SUM(asd) OVER (wnd)) * 1.0 / SUM(zxc) OVER (wnd))'
    assert metric.get_over('wnd') == res
