from collections.abc import Mapping, Iterable
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
        self.__cached_query = None

    def get_script(self, extender: str = '') -> str:
        if self.__cached_query is None:
            raw_query = self._read_query_from_file()
            self._ensure_must_have_keys(raw_query)
            self.__cached_query = raw_query.format(**self.format_params) + extender
        return self.__cached_query

    def _read_query_from_file(self) -> str:
        return Path(self._file_path).read_text(encoding='utf-8')

    def _ensure_must_have_keys(self, raw_query: str) -> None:
        keys = self.format_params.keys()
        if not all(f'{{{key}}}' in raw_query for key in keys):
            raise InvalidQueryKeyException(
                query=self._file_path,
                keys=keys,
            )


class SqlAlchemyQuery(SqlQuery):

    def get_script(self, extender: str = '') -> str:
        return text(super().get_script(extender))


class InvalidQueryKeyException(Exception):
    """
    Is thrown when the query doesn't contain required keys.
    """

    def __init__(self, query: str, keys: Iterable[str]) -> None:
        self.message = f'{query} must contain these keys: {", ".join(keys)}'
        Exception.__init__(self, self.message)
