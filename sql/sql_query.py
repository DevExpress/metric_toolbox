from typing import Dict, Iterable
from pathlib import Path


class SqlQuery:
    """
    Represents an interface to an sql query stored on disc.
    """

    def __init__(
        self,
        query_file_path: str,
        format_params: Dict[str, str],
        params: Iterable[Iterable] = None
    ) -> None:
        self._query_file_path = query_file_path
        self.format_params = format_params
        self.params = params
        self.__cached_query = None

    def get_query(self) -> str:
        if self.__cached_query is None:
            raw_query = self._read_query_from_file()
            self._ensure_must_have_keys(raw_query)
            self.__cached_query = raw_query.format(**self.format_params)
        return self.__cached_query

    def _read_query_from_file(self) -> str:
        return Path(self._query_file_path).read_text(encoding='utf-8')

    def _ensure_must_have_keys(self, raw_query: str) -> None:
        keys = self.format_params.keys()
        if not all(f'{{{key}}}' in raw_query for key in keys):
            raise InvalidQueryKeyException(
                query=self._query_file_path,
                keys=keys,
            )

    def get_params(self) -> Iterable:
        return self.params


class InvalidQueryKeyException(Exception):
    """
    Is thrown when the query doesn't contain required keys.
    """

    def __init__(self, query: str, keys: Iterable[str]) -> None:
        self.message = f'{query} must contain these keys: {", ".join(keys)}'
        Exception.__init__(self, self.message)
