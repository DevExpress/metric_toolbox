from collections.abc import Iterable
from toolbox.sql.generators.utils import multiline_non_empty


def create_index(
    tbl: str,
    cols: Iterable,
    *,
    name: str = '',
    unique: bool = False,
) -> str:
    cols = tuple(str(col) for col in cols)
    unq = 'UNIQUE ' if unique else ''
    name = name if name else "_".join(cols)
    return f'CREATE {unq}INDEX IF NOT EXISTS idx_{tbl}_{name} ON {tbl}({", ".join(cols)});'


def on_conflict(
    key_cols: Iterable[str],
    confilcting_cols: Iterable[str],
):
    if key_cols and confilcting_cols:
        return multiline_non_empty(
            f"ON CONFLICT({', '.join(key_cols)}) DO UPDATE SET",
            '\t\t' + ',\n\t\t'.join(
                f'{col}=excluded.{col}' for col in confilcting_cols
            ),
        )
    return 'ON CONFLICT DO NOTHING'


def with_median(tbl: str, group_by: str, group_by_fld: str, fld: str) -> str:
    return f"""(
SELECT  *,
    NTH_VALUE({fld}, median) OVER (PARTITION BY {group_by} ORDER BY {fld}) AS median_{fld}
    FROM    (   SELECT  {group_by_fld},
                        {fld},
                        ROUND(COUNT({fld}) OVER (PARTITION BY {group_by}) / 2.) AS median
                FROM    {tbl}
            ) AS tbl
) AS tbl_with_median"""
