import pytest
from typing import Dict, Any, Iterable
from pandas import DataFrame

from toolbox.sql.repository import Repository
from toolbox.sql.repository_queries import RepositoryQueries
from toolbox.sql.sql_query import SqlQuery
from toolbox.sql.query_executors.sqlite_query_executor import SQLiteQueryExecutor
from toolbox.sql.columns_validator import InvalidDataFormatException


def test_raise_exception_if_data_does_not_contain_required_columns():
    """
    GIVEN DataFrame with missing required column names
    WHEN obtained from the TribesRepository
    THEN it should raise InvalidDataFormatException
    """

    with pytest.MonkeyPatch.context() as monkeypatch:

        def mock_execute(
            prep_queries: Iterable[SqlQuery],
            main_query: SqlQuery,
            main_queries: Dict[str, SqlQuery] = {},
        ):
            data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
            return DataFrame(data=data)

        query_executor = SQLiteQueryExecutor()
        monkeypatch.setattr(
            query_executor,
            'execute',
            mock_execute,
        )
        columns = sorted(['asd', 'qwe'])
        with pytest.raises(InvalidDataFormatException) as exec_info:
            Repository(
                queries=RepositoryQueries(),
                query_executor=query_executor,
            ).get_data(
                query_file_path='',
                query_format_params='query_format_params',
                must_have_columns=columns,
            )
        columns_str = ', '.join(columns)
        assert exec_info.value.message == f'DataFrame must contain ({columns_str}) but got (col_1, col_2)'
