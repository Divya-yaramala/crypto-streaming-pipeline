from unittest.mock import patch

from consumer.portfolio_tracker import (
    calculate_portfolio_returns,
    calculate_portfolio_value,
    load_portfolio,
    save_portfolio_snapshot,
)

PORTFOLIO = {"bitcoin": 0.5, "ethereum": 0.3, "solana": 0.2}
CURRENT_PRICES = {"bitcoin": 65000.0, "ethereum": 3500.0, "solana": 150.0}
PREVIOUS_PRICES = {"bitcoin": 60000.0, "ethereum": 3000.0, "solana": 140.0}


def test_calculate_portfolio_value_total():
    metrics = calculate_portfolio_value(PORTFOLIO, CURRENT_PRICES)
    assert metrics["total_value"] == 10000.0


def test_calculate_portfolio_value_top_holding():
    metrics = calculate_portfolio_value(PORTFOLIO, CURRENT_PRICES)
    assert metrics["top_holding"] == "bitcoin"


def test_calculate_portfolio_returns_weighted():
    result = calculate_portfolio_returns(PORTFOLIO, CURRENT_PRICES, PREVIOUS_PRICES)
    btc_ret = (65000 - 60000) / 60000 * 100
    eth_ret = (3500 - 3000) / 3000 * 100
    sol_ret = (150 - 140) / 140 * 100
    expected = 0.5 * btc_ret + 0.3 * eth_ret + 0.2 * sol_ret
    assert abs(result["weighted_portfolio_return_pct"] - round(expected, 4)) < 1e-4


def test_load_portfolio_falls_back_to_default():
    with patch("consumer.portfolio_tracker.boto3.client") as mock_boto:
        mock_boto.return_value.get_object.side_effect = Exception("NoSuchBucket")
        result = load_portfolio("nonexistent-bucket")
    assert "bitcoin" in result
    assert result["bitcoin"] == 0.5


def test_save_portfolio_snapshot_no_bucket():
    result = save_portfolio_snapshot({"total_value": 10000}, bucket="", date="2026/06/21")
    assert result is False
