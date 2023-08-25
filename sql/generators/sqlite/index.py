from collections.abc import Iterable


def generate_create_index_statement(
    tbl: str,
    cols: Iterable,
    unique: bool = False,
) -> str:
    cols = tuple(str(col) for col in cols)
    unq = 'UNIQUE' if unique else ''
    return f'CREATE {unq} INDEX IF NOT EXISTS idx_{tbl}_{"_".join(cols)} ON {tbl}({",".join(cols)});'
