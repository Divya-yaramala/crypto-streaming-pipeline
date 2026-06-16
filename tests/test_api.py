import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi.testclient import TestClient

from api.main import app, get_db_connection


def _make_conn(rows=None):
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = rows or []
    cur.fetchone.return_value = None
    conn.cursor.return_value = cur
    return conn


def _override_db(rows=None, fetchone=None):
    conn = _make_conn(rows)
    if fetchone is not None:
        conn.cursor.return_value.fetchone.return_value = fetchone

    def _dep():
        yield conn

    return _dep


def test_health_endpoint():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert data["status"] == "healthy"


def test_get_cryptos():
    client = TestClient(app)
    response = client.get("/cryptos")
    assert response.status_code == 200
    cryptos = response.json()
    assert isinstance(cryptos, list)
    assert len(cryptos) == 5
    assert "bitcoin" in cryptos


def test_get_prices_valid_crypto():
    now = datetime.now(timezone.utc)
    rows = [("bitcoin", 65000.0, 1_200_000_000.0, 2.5, now, "coingecko")]
    app.dependency_overrides[get_db_connection] = _override_db(rows=rows)
    client = TestClient(app)
    response = client.get("/prices/bitcoin")
    app.dependency_overrides.clear()
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data[0]["crypto_id"] == "bitcoin"


def test_get_prices_invalid_crypto():
    app.dependency_overrides[get_db_connection] = _override_db(rows=[])
    client = TestClient(app)
    response = client.get("/prices/notacrypto")
    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json() == []


def test_get_alerts():
    now = datetime.now(timezone.utc)
    rows = [("bitcoin", "PUMP", "Price up more than 10% in 24h", 65000.0, now)]
    app.dependency_overrides[get_db_connection] = _override_db(rows=rows)
    client = TestClient(app)
    response = client.get("/alerts/bitcoin")
    app.dependency_overrides.clear()
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data[0]["alert_type"] == "PUMP"


def test_get_summary_structure():
    now = datetime.now(timezone.utc)
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.side_effect = [
        (65000.0, now),
        ("PUMP", "Price up", now),
        (64000.0, now),
    ]
    conn.cursor.return_value = cur

    def _dep():
        yield conn

    app.dependency_overrides[get_db_connection] = _dep
    client = TestClient(app)
    response = client.get("/summary/bitcoin")
    app.dependency_overrides.clear()
    assert response.status_code == 200
    data = response.json()
    assert "latest_price" in data
    assert "latest_alert" in data
    assert "recent_aggregation" in data
