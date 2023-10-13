from collections.abc import Sequence, Callable
from typing import Any, NamedTuple
from toolbox.sql.field import QueryField
from toolbox.sql.generators.sqlite.statements import generate_on_conflict, generate_drop_rows_older_than_trigger
import toolbox.logger as Logger


class DropRowsTriggerParams(NamedTuple):
    modifier: str = ''
    date_field: str = ''

    def __bool__(self):
        return self.modifier != '' and self.date_field != ''


class SqliteCreateTableQuery:

    def __init__(
        self,
        target_table_name: str,
        unique_key_fields: Sequence[QueryField] | None,
        values_fields: Sequence[QueryField] | None = None,
        *,
        keep_rows_for_last: DropRowsTriggerParams = DropRowsTriggerParams(),
        recreate: bool = False,
    ) -> None:
        self._target_table_name = target_table_name
        self._keys = unique_key_fields
        self._values = values_fields
        self._cached_query = None
        self._keep_rows_for_last = keep_rows_for_last
        self._recreate = recreate

    def get_table_name(self) -> str:
        return self._target_table_name

    def get_script(self, extender: str = '') -> str:
        if self._cached_query is None:
            keys, _, pk, without_rowid = self._get_keys()
            values, _ = self._get_values()
            self._cached_query = f'DROP TABLE IF EXISTS {self._target_table_name};\n' if self._recreate else ''
            self._cached_query += f'CREATE TABLE IF NOT EXISTS {self._target_table_name} ({keys}{values}{pk}){without_rowid};\n'
            self._cached_query += generate_drop_rows_older_than_trigger(
                tbl=self._target_table_name,
                date_field=self._keep_rows_for_last.date_field,
                modifier=self._keep_rows_for_last.modifier
            )
            self._cached_query += extender
        Logger.debug(self._cached_query)
        return self._cached_query

    def get_keys(self, projector: Callable[[QueryField], Any] = str):
        return [projector(x) for x in self._keys]

    def get_values(self, projector: Callable[[QueryField], Any] = str):
        return [projector(x) for x in self._values]

    def _get_keys(self):
        if not self._keys:
            return '', '', '\n', ''
        keys, keys_aliases = self.__get_fields_aliases(self._keys)
        keys += ','
        if self._values:
            keys_aliases += ','

        fields = ',\n\t\t'.join(str(val.target_name) for val in self._keys)
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
        *,
        keep_rows_for_last: DropRowsTriggerParams = DropRowsTriggerParams(),
        recreate: bool = True,
    ) -> None:
        self._source_table_or_subquery = source_table_or_subquery
        super().__init__(
            target_table_name=target_table_name,
            unique_key_fields=unique_key_fields,
            values_fields=values_fields,
            recreate=recreate,
            keep_rows_for_last=keep_rows_for_last,
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
                + f'{self.get_not_null_keys_filter()}\n'
                + self.get_on_conflict()
            )
        Logger.debug(self._cached_query)
        return self._cached_query

    def get_not_null_keys_filter(self):
        if self._keys:
            filter = ' AND \n'.join(
                f'{key.source_name} IS NOT NULL' for key in self._keys
            )
            return 'WHERE ' + filter
        return ''

    def get_on_conflict(self):
        return generate_on_conflict(
            key_cols=self.get_keys(lambda x: x.target_name),
            confilcting_cols=self.get_values(lambda x: x.target_name),
        )
