import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from toolbox.sql.connections.connection import (
    DbEngine,
    Connection,
)


_engine = None


def _get_or_create_engine(poolclass=None) -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            url=ConnectionParams().get_url(),
            poolclass=poolclass,  #NullPool,
        )
    return _engine


class ConnectionParams:

    def __init__(
        self,
        user_env: str = 'SQL_USER',
        password_env: str = 'SQL_PASSWORD',
        server_env: str = 'SQL_SERVER',
        data_base_env: str = 'SQL_DATABASE',
    ):
        self.user = os.environ[user_env]
        self.password = os.environ[password_env]
        self.server = os.environ[server_env]
        self.data_base = os.environ[data_base_env]

    def get_url(self):
        return (
            'mssql+pyodbc://' + self.user + ':' + self.password + '@'
            + self.server + '/' + self.data_base +
            '?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
        )


class SqlServerConnection(Connection):
    engine = _get_or_create_engine()

    def _get_or_create_engine(self) -> DbEngine:
        return SqlServerConnection.engine

    def dispose(self, **kwargs):
        SqlServerConnection.engine.dispose(close=True)
