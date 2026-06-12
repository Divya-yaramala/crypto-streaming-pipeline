import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage.snowflake_connector import (
    create_snowflake_objects,
    create_snowflake_tables,
    get_snowflake_connection,
)
from storage.snowflake_sync import (
    run_daily_mart_refresh,
    sync_alerts_to_snowflake,
    sync_prices_to_snowflake,
)


def _make_conn():
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    return conn, cur


def test_get_snowflake_connection_called_with_env():
    with patch("storage.snowflake_connector.snowflake") as mock_sf:
        with patch("storage.snowflake_connector.SNOWFLAKE_ACCOUNT", "test_account"):
            with patch("storage.snowflake_connector.SNOWFLAKE_USER", "test_user"):
                with patch("storage.snowflake_connector.SNOWFLAKE_PASSWORD", "test_pass"):
                    mock_sf.connector.connect.return_value = MagicMock()
                    get_snowflake_connection()
                    mock_sf.connector.connect.assert_called_once()
                    kwargs = mock_sf.connector.connect.call_args[1]
                    assert kwargs["account"] == "test_account"
                    assert kwargs["user"] == "test_user"


def test_create_snowflake_objects_executes_ddl():
    conn, cur = _make_conn()
    create_snowflake_objects(conn)
    executed = [c[0][0].strip() for c in cur.execute.call_args_list]
    assert any("CREATE DATABASE IF NOT EXISTS CRYPTO_PIPELINE_DB" in s for s in executed)
    assert any("CREATE SCHEMA IF NOT EXISTS CRYPTO_PIPELINE_DB.RAW" in s for s in executed)
    assert any("CREATE SCHEMA IF NOT EXISTS CRYPTO_PIPELINE_DB.MARTS" in s for s in executed)
    assert any("CREATE WAREHOUSE IF NOT EXISTS CRYPTO_PIPELINE_WH" in s for s in executed)


def test_create_snowflake_tables_creates_three_tables():
    conn, cur = _make_conn()
    create_snowflake_tables(conn)
    executed = [c[0][0] for c in cur.execute.call_args_list]
    assert any("RAW.CRYPTO_PRICES" in s for s in executed)
    assert any("RAW.CRYPTO_ALERTS" in s for s in executed)
    assert any("MARTS.CRYPTO_DAILY_SUMMARY" in s for s in executed)


def test_sync_prices_returns_zero_for_empty_list():
    conn, _ = _make_conn()
    result = sync_prices_to_snowflake(conn, [])
    assert result == 0


def test_run_daily_mart_refresh_success():
    conn, cur = _make_conn()
    result = run_daily_mart_refresh(conn)
    assert result is True
    cur.execute.assert_called_once()


def test_sync_alerts_inserts_alerts():
    conn, cur = _make_conn()
    cur.rowcount = 1
    alerts = [
        {
            "crypto_id": "ethereum",
            "alert_type": "PUMP",
            "message": "Price up more than 10% in 24h",
            "price_usd": 3200.0,
        }
    ]
    result = sync_alerts_to_snowflake(conn, alerts)
    assert result == 1
    cur.execute.assert_called_once()
