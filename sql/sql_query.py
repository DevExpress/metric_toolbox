from collections.abc import Mapping
from pathlib import Path
from sqlalchemy import text


class SqlQuery:
    """
    Represents an interface to an sql query stored on disc.
    """

    def __init__(
        self,
        query_file_path: str,
        format_params: Mapping[str, str],
    ) -> None:
        self._file_path = query_file_path
        self.format_params = format_params
        self._cached_query = None

    def __str__(self):
        return self._file_path

    def get_script(self, extender: str = '') -> str:
        if self._cached_query is None:
            raw_query = self._get_raw_query()
            self._cached_query = raw_query.format(**self.format_params) + extender
        return self._cached_query

    def _get_raw_query(self) -> str:
        return Path(self._file_path).read_text(encoding='utf-8')


class SqlAlchemyQuery(SqlQuery):

    def get_script(self, extender: str = '') -> str:
        return text(super().get_script(extender))


class GeneralSelectSqlQuery(SqlQuery):

    def __init__(
        self,
        format_params: Mapping[str, str],
    ) -> None:
        super().__init__('', format_params)

    def _get_raw_query(self) -> str:
        return '''
            SELECT {select}
            FROM {from}
            {where_group_limit}
        '''