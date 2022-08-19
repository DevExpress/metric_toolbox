import pytest
from sql.sqlite_data_base import SQLiteDataBase


def test_connections():
    with pytest.MonkeyPatch.context() as monkeypatch:
        prepare_environment(monkeypatch)
        db = SQLiteDataBase(name='test')
        db.try_connect()
        db.try_connect()
        assert db._connections_count == 2
        db.try_disconnect()
        assert db._connections_count == 1
        db.try_disconnect()
        assert db._connections_count == 0
        db.try_disconnect()
        assert db._connections_count == 0


def prepare_environment(monkeypatch: pytest.MonkeyPatch):

    def mock_connect(self):
        pass

    def mock_disconnect(self):
        pass

    monkeypatch.setattr(
        SQLiteDataBase,
        'connect',
        mock_connect,
    )

    monkeypatch.setattr(
        SQLiteDataBase,
        'disconnect',
        mock_disconnect,
    )