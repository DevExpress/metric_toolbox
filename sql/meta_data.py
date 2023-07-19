from collections.abc import Mapping, Iterable, Sequence


class MetaData:

    @classmethod
    def _get_dict(cls) -> Mapping[str, str]:
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
    def get_attrs(cls) -> Mapping[str, str]:
        return cls._get_dict()

    @classmethod
    def get_values(cls) -> Iterable[str]:
        return cls._get_dict().values()

    @classmethod
    def get_key_fields(cls) -> Sequence[str]:
        return []

    @classmethod
    def get_conflicting_fields(cls) -> Sequence[str]:
        key_fields = set(cls.get_key_fields())
        all_fields = set(cls.get_values())
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
