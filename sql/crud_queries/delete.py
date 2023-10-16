class DeleteRowsOlderThanQuery:

    def __init__(
        self,
        tbl: str,
        date_field: str,
        modifier: str = '1 YEARS',
    ) -> None:
        self._tbl = tbl
        self._date_field = date_field
        self._modifier = modifier
        self._cached_query = None

    def get_table_name(self) -> str:
        return self._tbl

    def get_script(self, extender: str = '') -> str:
        if self._cached_query is None:
            self._cached_query = f"DELETE FROM {self._tbl} WHERE {self._date_field} < (SELECT DATE(MAX({self._date_field}), '-{self._modifier}') FROM {self._tbl});"
        return self._cached_query

    def get_parameters(self):
        pass

    def __str__(self) -> str:
        return self.get_script()


class DropTableQuery:

    def __init__(
        self,
        tbl: str,
    ) -> None:
        self._tbl = tbl

    def get_table_name(self) -> str:
        return self._tbl

    def get_script(self, extender: str = '') -> str:
        return f'DROP TABLE IF EXISTS {self._tbl};'

    def get_parameters(self):
        pass

    def __str__(self) -> str:
        return self.get_script()
