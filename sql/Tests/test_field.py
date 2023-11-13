import pytest
from toolbox.sql.field import Field, QueryField, NUMERIC, TEXT


def test_query_field_str():
    assert str(
        QueryField(
            source_name='source_name',
            target_name='target_name',
            type=TEXT,
        )
    ) == f'target_name {TEXT}'


def test_query_field_as_alias():
    assert QueryField(
        source_name='source_name',
        target_name='target_name',
        type=TEXT,
    ).as_alias() == 'source_name AS target_name'


def test_query_field_eq():
    assert QueryField(
        source_name='source_name',
        target_name='target_name',
        type=TEXT,
    ) == QueryField(
        source_name='source_name',
        target_name='target_name',
        type=TEXT,
    )


def test_field_defaults():

    class tst:
        fld = Field()

    assert str(tst.fld) == 'fld'
    assert tst.fld.type == TEXT


def test_alias_defaults():

    class tst:
        fld = Field(alias='fld_alias')

    assert str(tst.fld) == 'fld_alias'


def test_sqlite_type_defaults():

    class tst:
        fld = Field(NUMERIC)

    assert tst.fld.type == NUMERIC


def test_as_query_field():

    class tst:
        fld = Field(NUMERIC)

    assert tst.fld.as_query_field() == QueryField(
        source_name='fld',
        target_name='fld',
        type=NUMERIC,
    )

    assert tst.fld.as_query_field('target_field') == QueryField(
        source_name='fld',
        target_name='target_field',
        type=NUMERIC,
    )
