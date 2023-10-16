import pytest
from toolbox.sql.crud_queries.protocols import CRUDQuery
from toolbox.sql.crud_queries.upsert import SqliteUpsertQuery


@pytest.mark.parametrize(
    'query, res', (
        (
            SqliteUpsertQuery(
                table_name='employees',
                cols=('id', 'name', 'pos'),
                key_cols=('id', ),
                confilcting_cols=('name', 'pos'),
                rows=((1, 'qwe', 'dev'),),
            ),
            (
'''INSERT INTO employees(id, name, pos)
VALUES(?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
		name=excluded.name,
		pos=excluded.pos'''
            ),
        ),
    )
)
def test_upsert(query: CRUDQuery, res: str):
    assert query.get_script() == res
