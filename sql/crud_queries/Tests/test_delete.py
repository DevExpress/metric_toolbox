import pytest
from toolbox.sql.crud_queries.protocols import CRUDQuery
from toolbox.sql.crud_queries.delete import DeleteRowsOlderThanQuery


@pytest.mark.parametrize(
    'query, res', (
        (
            DeleteRowsOlderThanQuery(
                tbl='tickets',
                date_field='date',
            ),
            f"DELETE FROM tickets WHERE date < (SELECT DATE(MAX(date), '-1 YEARS') FROM tickets);",
        ),
        (
            DeleteRowsOlderThanQuery(
                tbl='tickets',
                date_field='date',
                modifier='3 MONTHS'
            ),
            f"DELETE FROM tickets WHERE date < (SELECT DATE(MAX(date), '-3 MONTHS') FROM tickets);",
        ),
    )
)
def test_delete(query: CRUDQuery, res: str):
    assert query.get_script() == res
