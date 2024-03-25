import toolbox.config as config
from collections.abc import Iterable


def get_key(db_name, args: Iterable) -> str:
    db_name = db_name or config.redis_db()
    return f'{db_name}:{":".join(str(arg) for arg in args)}'
