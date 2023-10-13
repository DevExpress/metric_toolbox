from collections.abc import Sequence, Iterable
from toolbox.sql.generators.sqlite.statements import generate_on_conflict
import toolbox.logger as Logger

class SqliteUpsertQuery:

    def __init__(
        self,
        table_name: str,
        cols: Sequence[str],
        key_cols: Sequence[str],
        confilcting_cols: Sequence[str],
        rows: Iterable[Sequence],
    ) -> None:
        self._table_name = table_name
        self._cols = cols
        self._key_cols = key_cols
        self._confilcting_cols = confilcting_cols
        self._rows = rows
        self._cached_query = None

    def get_table_name(self) -> str:
        return self._table_name

    def get_script(self, extender: str = '') -> str:
        if self._cached_query is None:

            self._cached_query = f'''
                INSERT INTO {self._table_name}({', '.join(self._cols)})
                VALUES({', '.join('?' * len(self._cols))})
                {self.get_on_conflict()}
            '''
        Logger.debug(self._cached_query)
        return self._cached_query

    def get_on_conflict(self):
        return generate_on_conflict(
            key_cols=self._key_cols,
            confilcting_cols=self._confilcting_cols,
        )

    def get_parameters(self):
        return self._rows
