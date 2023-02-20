import os
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from toolbox.sql.connections.connection import (
    DbEngine,
    Connection,
)


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


_engine: Engine = None
if os.environ.get('PRODUCTION', None):
    _engine = create_engine(
        url=ConnectionParams().get_url(),
        #poolclass=NullPool,
        pool_reset_on_return=None,
    )

    @event.listens_for(_engine, 'reset')
    def reset_mssql(dbapi_connection, connection_record=None, reset_state = None):
        if not reset_state or not reset_state.terminate_only:
            dbapi_connection.execute('{call sys.sp_reset_connection}')
        dbapi_connection.rollback()


class SqlServerConnection(Connection):

    def _get_or_create_engine(self) -> DbEngine:
        return _engine
