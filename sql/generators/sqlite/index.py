from collections.abc import Iterable


def generate_create_index_statement(
    tbl: str,
    cols: Iterable[str],
    unique: bool = False,
) -> str:
    unq = 'UNIQUE' if unique else ''
    return f'CREATE {unq} INDEX IF NOT EXISTS idx_{tbl}_{"_".join(cols)} ON {tbl}({",".join(cols)});'
