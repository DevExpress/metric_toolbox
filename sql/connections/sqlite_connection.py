from functools import partial
from toolbox.sql.connections.connection import Connection
from toolbox.sql.sqlite_db import get_or_create_db


SqliteConnection = partial(Connection, db_engine=get_or_create_db())
