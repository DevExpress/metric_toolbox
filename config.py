import os
from collections.abc import Iterable
from toolbox.utils.converters import JSON_to_object


def sql_user() -> str:
    return os.environ['SQL_USER']


def sql_password() -> str:
    return os.environ['SQL_PASSWORD']


def sql_server() -> str:
    return os.environ['SQL_SERVER']


def sql_database() -> str:
    return os.environ['SQL_DATABASE']


def sqlite_database() -> str:
    return os.environ['SQLITE_DATABASE']


def redis_service() -> str:
    return os.environ['REDIS_SERVICE']


def redis_port() -> str:
    return os.environ['REDIS_PORT']


def redis_db() -> str:
    return os.environ['REDIS_DB']


def production() -> int:
    return int(os.environ.get('PRODUCTION', 0))


def debug() -> int:
    return int(os.environ.get('DEBUG', 0))


def auth_enabled() -> int:
    return int(os.environ.get('AUTH_ENABLED', 1))


def auth_endpoint() -> str:
    return os.environ['AUTH_ENDPOINT']


def cors_origins() -> Iterable[str]:
    return JSON_to_object.convert(os.environ['CORS_ORIGINS'])
