import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from unittest.mock import MagicMock, patch

from consumer.crypto_consumer import check_price_alert, create_kafka_consumer, save_to_postgres

SAMPLE_EVENT = {
    "crypto_id": "bitcoin",
    "price_usd": 65000.0,
    "market_cap_usd": 1280000000000.0,
    "change_24h_pct": 2.0,
    "timestamp": "2026-06-04T10:00:00+00:00",
    "source": "coingecko",
}


def _make_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


def test_save_to_postgres_success():
    conn, cursor = _make_conn()
    result = save_to_postgres(SAMPLE_EVENT, conn)
    assert result is True
    conn.commit.assert_called_once()
    cursor.execute.assert_called_once()


def test_save_to_postgres_failure():
    conn, cursor = _make_conn()
    cursor.execute.side_effect = Exception("DB error")
    result = save_to_postgres(SAMPLE_EVENT, conn)
    assert result is False
    conn.rollback.assert_called_once()


def test_check_price_alert_pump():
    conn, cursor = _make_conn()
    event = {**SAMPLE_EVENT, "change_24h_pct": 15.0}
    check_price_alert(event, conn)
    call_args = cursor.execute.call_args
    assert call_args is not None
    executed_sql = call_args[0][0]
    params = call_args[0][1]
    assert "INSERT INTO crypto_alerts" in executed_sql
    assert "PUMP" in params


def test_check_price_alert_dump():
    conn, cursor = _make_conn()
    event = {**SAMPLE_EVENT, "change_24h_pct": -15.0}
    check_price_alert(event, conn)
    call_args = cursor.execute.call_args
    assert call_args is not None
    params = call_args[0][1]
    assert "DUMP" in params


def test_check_price_alert_no_alert():
    conn, cursor = _make_conn()
    event = {**SAMPLE_EVENT, "change_24h_pct": 2.0}
    check_price_alert(event, conn)
    cursor.execute.assert_not_called()


def test_create_kafka_consumer_success():
    mock_consumer = MagicMock()
    with patch("consumer.crypto_consumer.KafkaConsumer", return_value=mock_consumer):
        result = create_kafka_consumer()
    assert result is mock_consumer
