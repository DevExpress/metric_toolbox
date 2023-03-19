from collections.abc import Mapping, Iterable


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
