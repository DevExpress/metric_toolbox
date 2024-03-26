from functools import partial
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from toolbox.sql.connections.connection import Connection
import toolbox.config as config


_engine: Engine = None
if config.production():

    url = (
        'mssql+pyodbc://' + config.sql_user() + ':' + config.sql_password()
        + '@' + config.sql_server() + '/' + config.sql_database()
        + '?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
    )

    _engine = create_engine(
        url=url,
        #poolclass=NullPool,
        pool_reset_on_return=None,
    )

    @event.listens_for(_engine, 'reset')
    def reset_mssql(
        dbapi_connection, connection_record=None, reset_state=None
    ):
        if not reset_state or not reset_state.terminate_only:
            dbapi_connection.execute('{call sys.sp_reset_connection}')
        dbapi_connection.rollback()


SqlServerConnection = partial(Connection, db_engine=_engine)
