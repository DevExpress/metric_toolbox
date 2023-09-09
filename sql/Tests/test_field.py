from toolbox.sql.field import Field, NUMERIC, TEXT


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
