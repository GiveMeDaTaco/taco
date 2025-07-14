import pytest
import teradatasql

from tlptaco.db.connection import DBConnection


class DummyConnect:
    last_kwargs = None


@pytest.fixture(autouse=True)
def patch_teradatasql_connect(monkeypatch):
    def fake_connect(**kwargs):
        DummyConnect.last_kwargs = kwargs
        return object()
    monkeypatch.setattr(teradatasql, 'connect', fake_connect)
    yield


def test_logmech_passed():
    conn = DBConnection(host="h", user="u", password="p", logmech="LDAP")
    conn.connect()
    assert DummyConnect.last_kwargs.get('logmech') == "LDAP"
    assert DummyConnect.last_kwargs.get('host') == "h"


def test_no_logmech_passed_when_none():
    conn = DBConnection(host="h2", user="u2", password="p2", logmech=None)
    conn.connect()
    assert 'logmech' not in DummyConnect.last_kwargs
