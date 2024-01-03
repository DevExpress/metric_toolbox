from collections.abc import Sequence, Callable
from typing import Any
from toolbox.sql.generators.utils import multiline_non_empty
from toolbox.sql.field import QueryField, Field
from toolbox.sql.generators.sqlite.statements import on_conflict
from toolbox.sql.crud_queries.delete import DropTableQuery
import toolbox.logger as Logger


class FieldForms:
    def __init__(self, fields: Sequence[QueryField]) -> None:
        if not fields:
            self.defs = ''
            self.names = ''
            self.aliases= ''
            return

        offset = '\n\t'
        separator = ',\n\t'
        self.defs = offset + separator.join(val.as_def() for val in fields)
        self.names = offset + separator.join(str(val) for val in fields)
        self.aliases = offset + separator.join(val.as_alias() for val in fields)


class SqliteCreateTableQuery:

    def __init__(
        self,
        target_table_name: str,
        unique_key_fields: Sequence[QueryField],
        values_fields: Sequence[QueryField] | None = None,
        *,
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
            self._cached_query = multiline_non_empty(
                self.__drop(),
                self.__create(),
                extender,
            )
        Logger.debug(self._cached_query)
        return self._cached_query

    def __drop(self):
        return str(DropTableQuery(self._target_table_name)) if self.recreate() else ''

    def __create(self):
        keys, pk, without_rowid = self._get_keys()
        values = self._get_values()
        return f'CREATE TABLE IF NOT EXISTS {self._target_table_name} ({keys.defs}{values.defs}{pk}){without_rowid};'

    def keys(self, projector: Callable[[QueryField], Any] = str):
        return [projector(x) for x in self._keys]

    def values(self, projector: Callable[[QueryField], Any] = str):
        return [projector(x) for x in self._values]

    def recreate(self):
        return self._recreate

    def _get_keys(self):
        forms = self.__to_field_forms(self._keys)
        if not self._keys:
            return forms, '\n', ''

        forms.defs += ','
        if self._values:
            forms.names += ','
            forms.aliases += ','

        fields = ',\n\t\t'.join(str(val.target_name) for val in self._keys)
        pk = f"\n\tPRIMARY KEY (\n\t\t{fields}\n\t)\n"

        return forms, pk, ' WITHOUT ROWID'

    def _get_values(self) -> FieldForms:
        forms = self.__to_field_forms(self._values)
        if self._values and self._keys:
            forms.defs += ','
        return forms

    def __to_field_forms(self, fields: Sequence[QueryField]) -> FieldForms:
        return FieldForms(fields)

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
        unique_fields: Sequence[Field] | None = None,
        recreate: bool = True,
    ) -> None:
        self._source_table_or_subquery = source_table_or_subquery
        self._unique_fields = unique_fields
        super().__init__(
            target_table_name=target_table_name,
            unique_key_fields=unique_key_fields or tuple(),
            values_fields=values_fields,
            recreate=recreate,
        )

    def get_script(self, extender: str = '') -> str:
        if self._cached_query is None:
            self._cached_query = multiline_non_empty(
                self.__create(extender),
                self.__upsert(),
            )
        Logger.debug(self._cached_query)
        return self._cached_query

    def __create(self, extender):
        return super().get_script(extender)

    def __upsert(self):
        keys, *_ = self._get_keys()
        values = self._get_values()
        return multiline_non_empty(
            f'INSERT INTO {self._target_table_name}({keys.names}{values.names}\n)',
            f'SELECT DISTINCT {keys.aliases}{values.aliases}',
            f'FROM {self._source_table_or_subquery}',
            self._where_keys_not_null(),
            self._on_conflict(),
        )

    def _where_keys_not_null(self):
        keys = self.keys(lambda x: x.source_name) or self._unique_fields
        if keys:
            filter = ' AND \n'.join(f'{key} IS NOT NULL AND {key} != ""' for key in keys)
            return 'WHERE ' + filter
        return ''

    def _on_conflict(self):
        if self.recreate():
            return ''
        return on_conflict(
            key_cols=self._unique_fields or self.keys(lambda x: x.target_name),
            confilcting_cols=self.values(lambda x: x.target_name),
        )
