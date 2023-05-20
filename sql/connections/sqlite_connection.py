import os
import sqlite3
from functools import partial
from toolbox.sql.connections.connection import Connection


class SQLiteDataBase:

    def begin(self):
        return sqlite3.connect(database=os.environ['SQLITE_DATABASE'])


SqliteConnection = partial(Connection, db_engine=SQLiteDataBase())
