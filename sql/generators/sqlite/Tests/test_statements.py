import pytest
from toolbox.sql.generators.sqlite.statements import on_conflict, create_index, with_median


@pytest.mark.parametrize(
    'tbl, cols, name, unique, res', [
        (
            'tbl', ['col1', 'col2'], '', False,
            'CREATE INDEX IF NOT EXISTS idx_tbl_col1_col2 ON tbl(col1, col2);'
        ),
        (
            'tbl', ['col1'], 'name', False,
            'CREATE INDEX IF NOT EXISTS idx_tbl_name ON tbl(col1);'
        ),
        (
            'tbl', ['col1'], 'name', True,
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_tbl_name ON tbl(col1);'
        ),
    ]
)
def test_create_index(tbl, cols, name, unique, res):
    assert create_index(tbl, cols, name=name, unique=unique) == res


@pytest.mark.parametrize(
    'key_cols, val_cols, res', [
        (
            [],
            ['col1'],
            'ON CONFLICT DO NOTHING',
        ),
        (
            ['col1'],
            [],
            'ON CONFLICT DO NOTHING',
        ),
        (
            [],
            [],
            'ON CONFLICT DO NOTHING',
        ),
        (
            ['key_col1', 'key_col2'],
            ['col1', 'col2'],
            'ON CONFLICT(key_col1, key_col2) DO UPDATE SET\n\t\tcol1=excluded.col1,\n\t\tcol2=excluded.col2',
        ),
    ]
)
def test_on_conflict(key_cols, val_cols, res):
    assert on_conflict(key_cols, val_cols) == res


@pytest.mark.parametrize(
    'tbl, group_by, group_by_fld, fld, res', [
        (
            'tbl',
            'group_by',
            'group_by_fld',
            'fld',
            """(
SELECT  *,
    NTH_VALUE(fld, median) OVER (PARTITION BY group_by ORDER BY fld) AS median_fld
    FROM    (   SELECT  group_by_fld,
                        fld,
                        ROUND(COUNT(fld) OVER (PARTITION BY group_by) / 2.) AS median
                FROM    tbl
            ) AS tbl
) AS tbl_with_median""",
        ),
    ]
)
def test_with_median(tbl, group_by, group_by_fld, fld, res):
    assert with_median(
        tbl=tbl,
        group_by=group_by,
        group_by_fld=group_by_fld,
        fld=fld,
    ) == res
