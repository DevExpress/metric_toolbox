from collections.abc import Mapping, Iterable, Sequence, Callable
from typing import Any, TypeVar
from itertools import chain
from toolbox.sql.field import Field, TEXT, INTEGER


Field_T = TypeVar('Field_T', Field, str)
Projector_T = TypeVar(
    'Projector_T',
    bound=Callable[[Field | str], Field | str | Any],
)


class __meta(type):

    def __str__(self) -> str:
        """
        This allows us to convert MetaData into str of
        attr values separated by commas.
        str(MetaDataSubClass) = 'field1, field2, etc'
        """
        return ', '.join(self.get_values())


class MetaData(metaclass=__meta):
    """
    Describes a table.
    Attrs of the Field or str type describe table columns.
    This class may be used for creating tables, validating sql query columns etc.
    """

    @classmethod
    def _get_dict(cls) -> Mapping[str, Field_T]:
        """
        Returns attrs collected from the whole inheritace chain
        in the subclass -> superclass order.
        Attrs starting with _ or get_ are ignored.
        """
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
        """
        Returns attr {name:value} pairs.
        """
        return cls._get_dict()

    @classmethod
    def get_values(
        cls,
        projector: Projector_T = str,
    ) -> Sequence[Field_T | Any]:
        """
        Returns attr values.
        projector is used for projecting attr values into required form. 
        """
        return _apply(projector, cls._get_dict().values())

    @classmethod
    def get_index_fields(
        cls,
        projector: Projector_T = str,
        *exfields: Field_T,
    ) -> Sequence[Field_T | Any]:
        """"
        Returns fields which may by used for redundant index creation.
        projector is used for projecting attr values into required form.
        exfields is meant for adding additional fields to the index on the fly.
        """
        return _apply(projector, exfields)

    @classmethod
    def get_key_fields(
        cls,
        projector: Projector_T = str,
        *exfields: Field_T,
    ) -> Sequence[Field_T | Any]:
        """
        Returns fields which may by used for clustered index creation.
        projector is used for projecting attr values into required form.
        exfields is meant for adding additional fields to the index on the fly.
        """
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
        preserve_order: bool = False
    ) -> Sequence[Field_T | Any]:
        """
        Returns non clustered index fields.
        projector is used for projecting attr values into required form.
        preserve_order alows preserving the attr declaration top to bottom order.
        """
        if preserve_order:
            keys = cls.get_key_fields(str)
            return _apply(
                projector,
                (
                    field for field in cls.get_values(lambda x: x)
                    if str(field) not in keys
                ),
            )
        key_fields = set(cls.get_key_fields(projector))
        all_fields = set(cls.get_values(projector))
        return all_fields - key_fields

    @classmethod
    def get_name(cls) -> str:
        """
        Returns table name.
        """
        return cls.__name__
    
    @classmethod
    def get_alias(cls) -> str:
        """
        Returns table alias.
        """
        return cls.get_name()

    @classmethod
    def get_indices(cls) -> Sequence[str]:
        """
        Returns index statements to be executed.
        """
        return tuple()


class KnotMeta(MetaData):
    id = Field(TEXT)
    name = Field(TEXT)


class IntKnotMeta(MetaData):
    id = Field(INTEGER)
    name = Field(TEXT)


class ValidationMeta(MetaData):
    value = Field(TEXT)
    valid = Field(INTEGER)


class MetricAggMeta(MetaData):
    period = Field(TEXT)
    agg = Field(TEXT)
    name = Field(TEXT)


class PeriodMeta(MetaData):
    start = Field(TEXT)
    end = Field(TEXT)


def _apply(f: Callable, iter: Iterable) -> Sequence:
    return tuple(f(x) for x in iter)
