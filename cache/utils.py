import os
from collections.abc import Iterable


def get_key(db_name, args: Iterable) -> str:
    db_name = db_name or os.environ['REDIS_DB']
    return f'{db_name}:{":".join(str(arg) for arg in args)}'
