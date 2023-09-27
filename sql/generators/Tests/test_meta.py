import pytest
from collections.abc import Sequence
from toolbox.sql.meta_data import MetaData
from toolbox.sql.field import Field


class Meta(MetaData):
    id = Field()
    name = Field()

    @classmethod
    def get_any(cls):
        ...


class NewMeta(Meta):
    value = Field()
    status = Field()

    @classmethod
    def get_index_fields(
        cls,
        projector=str,
        *exfields,
    ) -> Sequence[Field]:
        return MetaData.get_index_fields(
            projector,
            cls.id,
            *exfields,
        )


@pytest.fixture
def meta():
    return Meta


@pytest.fixture
def new_meta():
    return NewMeta


def test_str(meta: Meta):
    assert str(meta) == 'id, name'


def test_get_attrs(meta: Meta):
    assert meta.get_attrs() == {'id': meta.id, 'name': meta.name}


def test_get_values(meta: Meta):
    assert meta.get_values() == ('id', 'name')
    assert meta.get_values(projector=lambda x: f'*{x}*') == ('*id*', '*name*')


# yapf: disable
def test_get_index_fields(meta: Meta, new_meta: NewMeta):
    assert meta.get_index_fields() == tuple()
    assert meta.get_index_fields(str, Field(alias='custom'), Field(alias='id')) == ('custom', 'id')
    assert new_meta.get_index_fields() == ('id',)
    assert new_meta.get_index_fields(str, Field(alias='custom')) == ('id', 'custom')


def test_get_key_fields(meta: Meta, new_meta: NewMeta):
    assert meta.get_key_fields() == ('id', 'name')
    assert meta.get_key_fields(str, Field(alias='custom')) == ('id', 'name', 'custom')
    assert new_meta.get_key_fields() == ('id',)
    assert new_meta.get_key_fields(str, Field(alias='custom')) == ('id', 'custom')


def test_get_conflicting_fields(meta: Meta, new_meta: NewMeta):
    assert meta.get_conflicting_fields() == set()
    assert new_meta.get_conflicting_fields() == {'name', 'value', 'status'}
    assert new_meta.get_conflicting_fields(preserve_order=True) == ('value', 'status', 'name')
# yapf: enable
