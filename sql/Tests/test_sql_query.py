import pytest

from sql.sql_query import SqlQuery, InvalidQueryKeyException


def test_raise_exception_if_get_query_does_not_contain_required_keys():
    """
    GIVEN missing requred format params
    WHEN creating a new SqlQuery
    THEN it should raise InvalidQueryKeyException
    """

    with pytest.MonkeyPatch.context() as monkeypatch:

        def mock_read_query_from_file(self):
            return 'query_str'

        sql_query = SqlQuery(
            query_file_path='test_query',
            format_params={
                'ds': 'ds',
                'y': 'y',
                'tribe_id': 't',
                'years_ago': 3,
            }
        )
        monkeypatch.setattr(
            SqlQuery,
            '_read_query_from_file',
            mock_read_query_from_file,
        )
        with pytest.raises(InvalidQueryKeyException) as exec_info:
            sql_query.get_query()
        assert exec_info.value.message == 'test_query must contain these keys: ds, y, tribe_id, years_ago'
