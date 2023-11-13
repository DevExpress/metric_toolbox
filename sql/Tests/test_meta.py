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

    @classmethod
    def _test_fields(cls, projector=lambda x: x):
        return tuple(projector(x) for x in (cls.id, cls.name))

    @classmethod
    def _test_index_fields(cls, *_):
        return tuple()

    @classmethod
    def _test_key_fields(cls, projector=lambda x: x):
        return cls._test_index_fields(projector) or cls._test_fields(projector)

    @classmethod
    def _test_conflicting_fields(cls, *_):
        return tuple()

    @classmethod
    def _test_str(cls):
        return ', '.join(str(x) for x in cls._test_fields())

    @classmethod
    def _test_tuple(cls, projector=str):
        return cls._test_fields(projector)

    @classmethod
    def _test_dict(cls):
        return {str(x): x for x in cls._test_fields()}


class MetaWithIndexFields(Meta):
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

    @classmethod
    def _test_fields(cls, projector=lambda x: x):
        return tuple(
            projector(x) for x in (cls.value, cls.status, cls.id, cls.name)
        )

    @classmethod
    def _test_index_fields(cls, projector=lambda x: x):
        return tuple(projector(x) for x in (cls.id, ))

    @classmethod
    def _test_conflicting_fields(cls, projector=lambda x: x):
        return tuple(projector(x) for x in (cls.value, cls.status, cls.name))

    @classmethod
    def _test_str(cls):
        return ', '.join(cls._test_fields(str))

    @classmethod
    def _test_tuple(cls, projector=str):
        return cls._test_fields(projector)

    @classmethod
    def _test_dict(cls):
        return {str(x): x for x in cls._test_fields()}


class MetaWithoutKeyFields(MetaWithIndexFields):

    @classmethod
    def get_key_fields(
        cls,
        projector=str,
        *exfields,
    ) -> Sequence[Field]:
        return MetaData.get_key_fields(
            projector,
            *exfields,
        )

    @classmethod
    def _test_key_fields(cls, *_):
        return tuple()


@pytest.fixture
def meta():
    return Meta


@pytest.fixture
def new_with_index_keys():
    return MetaWithIndexFields


@pytest.fixture
def meta_without_key_fields():
    return MetaWithoutKeyFields


@pytest.mark.parametrize(
    'meta',
    (
        (Meta),
        (MetaWithIndexFields),
        (MetaWithoutKeyFields),
    ),
)
def test_str(meta: Meta):
    assert str(meta) == meta._test_str()


@pytest.mark.parametrize(
    'meta',
    (
        (Meta),
        (MetaWithIndexFields),
        (MetaWithoutKeyFields),
    ),
)
def test_get_attrs(meta: MetaData):
    assert meta.get_attrs() == meta._test_dict()


@pytest.mark.parametrize(
    'meta, projector', (
        (Meta, None),
        (Meta, lambda x: f'*{x}*'),
        (MetaWithIndexFields, None),
        (MetaWithIndexFields, lambda x: f'*{x}*'),
        (MetaWithoutKeyFields, None),
        (MetaWithoutKeyFields, lambda x: f'*{x}*'),
    )
)
def test_get_values(meta: Meta, projector):
    if projector:
        assert meta.get_values(projector=projector) == meta._test_tuple(projector=projector)
    else:
        assert meta.get_values() == meta._test_tuple()


@pytest.mark.parametrize(
    'meta, projector', (
        (Meta, str),
        (Meta, lambda x: f'*{x}*'),
        (MetaWithIndexFields, str),
        (MetaWithIndexFields, lambda x: f'*{x}*'),
        (MetaWithoutKeyFields, str),
        (MetaWithoutKeyFields, lambda x: f'*{x}*'),
    )
)
def test_get_index_fields(meta: Meta, projector):
    res = meta._test_index_fields(projector)
    custom_fields = [Field(alias='custom'), Field(alias='custom1')]

    assert meta.get_index_fields(projector=projector) == res
    assert meta.get_index_fields(projector, *custom_fields) == (*res, *(projector(x) for x in custom_fields))


@pytest.mark.parametrize(
    'meta, projector', (
        (Meta, str),
        (Meta, lambda x: f'*{x}*'),
        (MetaWithIndexFields, str),
        (MetaWithIndexFields, lambda x: f'*{x}*'),
        (MetaWithoutKeyFields, str),
        (MetaWithoutKeyFields, lambda x: f'*{x}*'),
    )
)
def test_get_key_fields(meta: Meta, projector):
    custom_fields = [Field(alias='custom')]

    assert meta.get_key_fields() == meta._test_key_fields(str)
    assert meta.get_key_fields(projector, *custom_fields) == (
        *meta._test_key_fields(projector),
        *(projector(x) for x in custom_fields)
    )


@pytest.mark.parametrize(
    'meta, projector', (
        (Meta, str),
        (Meta, lambda x: f'*{x}*'),
        (MetaWithIndexFields, str),
        (MetaWithIndexFields, lambda x: f'*{x}*'),
        (MetaWithoutKeyFields, str),
        (MetaWithoutKeyFields, lambda x: f'*{x}*'),
    )
)
def test_get_conflicting_fields(meta: Meta, projector):
    assert meta.get_conflicting_fields(projector=projector) == set(meta._test_conflicting_fields(projector))
    assert meta.get_conflicting_fields(projector=projector, preserve_order=True) == meta._test_conflicting_fields(projector)
