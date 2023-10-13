from collections.abc import Iterable


def generate_create_index_statement(
    tbl: str,
    cols: Iterable,
    *,
    name: str = '',
    unique: bool = False,
) -> str:
    cols = tuple(str(col) for col in cols)
    unq = 'UNIQUE' if unique else ''
    name = name if name else "_".join(cols)
    res = f'CREATE {unq} INDEX IF NOT EXISTS idx_{tbl}_{name} ON {tbl}({",".join(cols)});'
    return res


def generate_on_conflict(
    key_cols: Iterable[str],
    confilcting_cols: Iterable[str],
):
    confilcting_cols = ',\n'.join(
        f'{col}=excluded.{col}' for col in confilcting_cols
    )
    if confilcting_cols:
        return f'''ON CONFLICT({', '.join(key_cols)}) DO UPDATE SET
                {confilcting_cols}'''
    return 'ON CONFLICT DO NOTHING'


def generate_drop_rows_older_than_trigger(
    tbl: str,
    date_field: str,
    modifier: str = '1 YEARS',
):
    '''modifier can be one of NNN (DAYS | HOURS | MINUTES | SECONDS | MONTHS | YEARS)
    ex. 1 YEARS
    '''
    if not date_field or not modifier:
        return ''

    return f'''
CREATE TRIGGER IF NOT EXISTS trgr{tbl} AFTER INSERT ON {tbl}
BEGIN
    DELETE FROM {tbl} WHERE {date_field} < (SELECT DATE(MAX({date_field}), '-{modifier}') from {tbl});
END;
'''
