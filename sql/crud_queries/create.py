from collections.abc import Iterable
from typing import NamedTuple, Literal


class QueryField(NamedTuple):
    source_name: str
    target_name: str
    type: Literal['INTEGER', 'TEXT']

    def __str__(self) -> str:
        return f'{self.target_name} {self.type}'

    def as_alias(self) -> str:
        return f'{self.source_name} AS {self.target_name}'


class SqliteCreateTableFromTableQuery:

    def __init__(
        self,
        source_table_or_subquery: str,
        target_table_name: str,
        unique_key: QueryField | None,
        values: Iterable[QueryField] | None = None,
    ) -> None:
        self._source_table_or_subquery = source_table_or_subquery
        self._target_table_name = target_table_name
        self._key = unique_key
        self._values = values
        self._cached_query = None

    def get_table_name(self) -> str:
        return self._target_table_name

    def get_script(self, extender: str = '') -> str:
        if self._cached_query is None:
            key, key_alias, without_rowid = self.__get_keys()
            values, values_alias = self.__get_values()

            self._cached_query = f'''
            DROP TABLE IF EXISTS {self._target_table_name};
            CREATE TABLE {self._target_table_name}({key}{values}) {without_rowid};

            INSERT INTO {self._target_table_name}
            SELECT DISTINCT {key_alias}{values_alias}
            FROM {self._source_table_or_subquery}
            '''
        return self._cached_query

    def __get_keys(self):
        if not self._key:
            return '', '', ''
        key = f'{self._key} PRIMARY KEY'
        key_alias = self._key.as_alias()
        if self._values:
            key += ','
            key_alias += ','
        return key, key_alias, 'WITHOUT ROWID'

    def __get_values(self):
        if not self._values:
            return '', ''
        values = '\n' + ',\n'.join(str(val) for val in self._values) + '\n'
        values_alias = '\n' + ',\n'.join(val.as_alias() for val in self._values)
        return values, values_alias

    def get_parameters(self) -> None:
        pass
