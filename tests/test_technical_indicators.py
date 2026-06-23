from consumer.technical_indicators import (
    calculate_bollinger_bands,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
    run_technical_analysis,
)

PRICES_30 = [float(100 + i) for i in range(30)]
PRICES_5 = [100.0, 102.0, 101.0, 103.0, 104.0]


def test_sma_correct_length():
    result = calculate_sma(PRICES_30, window=5)
    assert len(result) == len(PRICES_30) - 5 + 1


def test_sma_insufficient_data_returns_empty():
    result = calculate_sma(PRICES_5, window=10)
    assert result == []


def test_rsi_values_in_range():
    prices = [float(100 + (i % 5)) for i in range(30)]
    result = calculate_rsi(prices, period=14)
    assert len(result) > 0
    assert all(0.0 <= v <= 100.0 for v in result)


def test_bollinger_bands_upper_above_lower():
    result = calculate_bollinger_bands(PRICES_30, window=10)
    assert len(result["upper"]) == len(result["lower"]) == len(result["middle"])
    for u, l in zip(result["upper"], result["lower"]):
        assert u >= l


def test_macd_returns_three_series():
    prices = [float(100 + i * 0.5) for i in range(50)]
    result = calculate_macd(prices, fast=5, slow=10, signal=3)
    assert len(result["macd"]) == len(result["signal"]) == len(result["histogram"]) == 50


def test_run_technical_analysis_no_bucket_skips_s3():
    from unittest.mock import patch

    with patch("consumer.technical_indicators.boto3.client") as mock_boto:
        result = run_technical_analysis("bitcoin", PRICES_30, bucket="")
    mock_boto.assert_not_called()
    assert result["crypto_id"] == "bitcoin"
    assert result["price_count"] == 30
