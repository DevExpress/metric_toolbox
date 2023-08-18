from collections.abc import Sequence
from typing import NamedTuple, Literal


class QueryField(NamedTuple):
    source_name: str
    target_name: str
    type: Literal['INTEGER', 'TEXT']

    def __str__(self) -> str:
        return f'{self.target_name} {self.type}'

    def as_alias(self) -> str:
        return f'{self.source_name} AS {self.target_name}'


class SqliteCreateTableQuery:

    def __init__(
        self,
        target_table_name: str,
        unique_key_fields: Sequence[QueryField] | None,
        values_fields: Sequence[QueryField] | None = None,
        recreate: bool = False,
    ) -> None:
        self._target_table_name = target_table_name
        self._keys = unique_key_fields
        self._values = values_fields
        self._cached_query = None
        self._recreate = recreate

    def get_table_name(self) -> str:
        return self._target_table_name

    def get_script(self, extender: str = '') -> str:
        if self._cached_query is None:
            keys, _, pk, without_rowid = self._get_keys()
            values, _ = self._get_values()
            self._cached_query = f'DROP TABLE IF EXISTS {self._target_table_name};\n' if self._recreate else ''
            self._cached_query += f'CREATE TABLE IF NOT EXISTS {self._target_table_name} ({keys}{values}{pk}){without_rowid};\n'
            self._cached_query += extender
        return self._cached_query

    def _get_keys(self):
        if not self._keys:
            return '', '', '\n', ''
        keys, keys_aliases = self.__get_fields_aliases(self._keys)
        keys += ','
        if self._values:
            keys_aliases += ','

        fields = ',\n\t\t'.join(val.target_name for val in self._keys)
        pk = f"\n\tPRIMARY KEY (\n\t\t{fields}\n\t)\n"

        return keys, keys_aliases, pk, ' WITHOUT ROWID'

    def _get_values(self):
        if not self._values:
            return '', ''
        values, values_aliases = self.__get_fields_aliases(self._values)
        if self._keys:
            values += ','
        return values, values_aliases

    def __get_fields_aliases(self, fields: Sequence[QueryField]):
        flds = '\n\t' + ',\n\t'.join(str(val) for val in fields)
        aliases = '\n\t' + ',\n\t'.join(val.as_alias() for val in fields)
        return flds, aliases

    def get_parameters(self) -> None:
        pass


class SqliteCreateTableFromTableQuery(SqliteCreateTableQuery):

    def __init__(
        self,
        source_table_or_subquery: str,
        target_table_name: str,
        unique_key_fields: Sequence[QueryField] | None,
        values_fields: Sequence[QueryField] | None = None,
    ) -> None:
        self._source_table_or_subquery = source_table_or_subquery
        super().__init__(
            target_table_name=target_table_name,
            unique_key_fields=unique_key_fields,
            values_fields=values_fields,
            recreate=True,
        )

    def get_script(self, extender: str = '') -> str:
        if self._cached_query is None:
            self._cached_query = super().get_script(extender) + '\n'
            _, key_alias, *_ = self._get_keys()
            _, values_aliases = self._get_values()

            self._cached_query += (
                f'INSERT INTO {self._target_table_name}\n'
                + f'SELECT DISTINCT {key_alias}{values_aliases}\n'
                + f'FROM {self._source_table_or_subquery}\n'
                + self.get_not_null_filter()
            )
        return self._cached_query

    def get_not_null_filter(self):
        if self._keys:
            filter = ' AND \n'.join(
                f'{key.source_name} IS NOT NULL' for key in self._keys
            )
            return 'WHERE ' + filter
        return ''
