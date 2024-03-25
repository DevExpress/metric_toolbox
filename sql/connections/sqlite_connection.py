import sqlite3
import toolbox.config as config
from functools import partial
from toolbox.sql.connections.connection import Connection


class SQLiteDataBase:

    def begin(self):
        return sqlite3.connect(database=config.sqlite_database())


SqliteConnection = partial(Connection, db_engine=SQLiteDataBase())
