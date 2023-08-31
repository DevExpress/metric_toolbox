from collections.abc import Mapping, Iterable, Sequence, Callable
from typing import Literal, NamedTuple, Any, TypeVar
from itertools import chain


class Field(NamedTuple):
    name: str
    sqlite_type: Literal['TEXT', 'INTEGER', 'REAL']

    def __str__(self) -> str:
        return self.name


Field_T = TypeVar('Field_T', Field, str)
Projector_T = TypeVar(
    'Projector_T',
    bound=Callable[[Field | str], Field | str | Any],
)


class MetaData:

    @classmethod
    def _get_dict(cls) -> Mapping[str, Field_T]:
        res = {}
        while cls:
            res.update(
                {
                    k: v
                    for k, v in cls.__dict__.items()
                    if not k.startswith('_') and not k.startswith('get_')
                }
            )
            cls = cls.__base__
        return res

    @classmethod
    def get_attrs(cls) -> Mapping[str, Field_T]:
        return cls._get_dict()

    @classmethod
    def get_values(
        cls,
        projector: Projector_T = str,
    ) -> Sequence[Field_T | Any]:
        return _apply(projector, cls._get_dict().values())

    @classmethod
    def get_index_fields(
        cls,
        projector: Projector_T = str,
        *exfields: Field_T,
    ) -> Sequence[Field_T | Any]:
        return _apply(projector, exfields)

    @classmethod
    def get_key_fields(
        cls,
        projector: Projector_T = str,
        *exfields: Field_T,
    ) -> Sequence[Field_T | Any]:
        return tuple(
            chain(
                cls.get_index_fields(projector) or cls.get_values(projector),
                _apply(projector, exfields),
            )
        )

    @classmethod
    def get_conflicting_fields(
        cls,
        projector: Projector_T = str,
    ) -> Sequence[Field_T | Any]:
        key_fields = set(cls.get_key_fields(projector))
        all_fields = set(cls.get_values(projector))
        return all_fields - key_fields


class KnotMeta(MetaData):
    id = 'id'
    name = 'name'


class ValidationMeta(MetaData):
    value = 'value'
    valid = 'valid'


class MetricAggMeta(MetaData):
    period = 'period'
    agg = 'agg'
    name = 'name'


class PeriodMeta(MetaData):
    start = 'start'
    end = 'end'


def _apply(f: Callable, iter: Iterable) -> Sequence:
    return tuple(f(x) for x in iter)
