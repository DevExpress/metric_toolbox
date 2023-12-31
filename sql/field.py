from typing import Union, NamedTuple


TEXT = 'TEXT'
INTEGER = 'INTEGER'
REAL = 'REAL'
NUMERIC = 'NUMERIC'


class Field:
    """
    SQLITE column descriptor which is used as part of create table statements:
    CREATE TABLE IF NOT EXISTS table_name (
        col1 type,
        col2 type
    );
    """
    __slots__ = ('name', 'type')

    def __init__(
        self,
        type: Union[TEXT, INTEGER, REAL, NUMERIC] = TEXT,
        *,
        alias: str | None = None,
    ) -> None:
        self.type = type
        self.name = alias

    def __str__(self) -> str:
        return self.name

    def __set_name__(self, owner, name):
        if self.name:
            return
        self.name = name

    def as_query_field(
        self,
        target_field: Union[str, 'Field'] = '',
    ) -> 'QueryField':
        return QueryField(
            source_name=self.name,
            target_name=str(target_field) or self.name,
            type=self.type,
        )


class QueryField(NamedTuple):
    """
    SQLITE column descriptor which is used as part of create / insert into table from table statements:

    CREATE TABLE IF NOT EXISTS table_name (
        col1 type,
        col2 type
    );

    INSERT INTO target_table(
        target_col
    )
    SELECT  source_col AS target_col
    FROM    source_table
    """
    source_name: str
    target_name: str
    type: Union[TEXT, INTEGER, REAL, NUMERIC]

    def __str__(self) -> str:
        return self.target_name

    def as_def(self) -> str:
        return f'{self.target_name} {self.type}'

    def as_alias(self) -> str:
        return f'{self.source_name} AS {self.target_name}'

    def __eq__(self, other: 'QueryField') -> bool:
        return (
            self.source_name == other.source_name
            and self.target_name == other.target_name
            and self.type == other.type
        )
