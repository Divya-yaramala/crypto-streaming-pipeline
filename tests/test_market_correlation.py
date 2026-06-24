import json
from unittest.mock import MagicMock, patch

import pandas as pd

from consumer.market_correlation import (
    calculate_beta,
    calculate_correlation_matrix,
    find_highly_correlated,
    load_price_series,
)

CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]

SAMPLE_PRICES = pd.DataFrame(
    {
        "bitcoin": [60000.0, 61000.0, 62000.0, 61500.0, 63000.0],
        "ethereum": [3000.0, 3050.0, 3100.0, 3075.0, 3150.0],
        "solana": [150.0, 152.0, 155.0, 153.0, 157.0],
        "cardano": [0.50, 0.51, 0.52, 0.515, 0.53],
        "dogecoin": [0.12, 0.122, 0.124, 0.123, 0.126],
    }
)


def test_calculate_correlation_matrix_shape():
    corr = calculate_correlation_matrix(SAMPLE_PRICES)
    assert corr.shape == (5, 5)


def test_correlation_diagonal_is_one():
    corr = calculate_correlation_matrix(SAMPLE_PRICES)
    for col in corr.columns:
        assert abs(corr.loc[col, col] - 1.0) < 1e-6


def test_find_highly_correlated_threshold():
    corr = calculate_correlation_matrix(SAMPLE_PRICES)
    pairs = find_highly_correlated(corr, threshold=0.8)
    assert isinstance(pairs, list)
    for pair in pairs:
        assert pair["correlation"] > 0.8
        assert "crypto1" in pair
        assert "crypto2" in pair


def test_calculate_beta_positive():
    prices = pd.DataFrame(
        {
            "bitcoin": [60000.0, 61000.0, 62000.0, 63000.0, 64000.0],
            "ethereum": [3000.0, 3060.0, 3120.0, 3180.0, 3240.0],
        }
    )
    beta = calculate_beta("ethereum", "bitcoin", prices)
    assert isinstance(beta, float)
    assert beta > 0


def test_load_price_series_structure():
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps({"price_usd": 65000.0}).encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}

    with patch("consumer.market_correlation.boto3.client", return_value=mock_s3):
        df = load_price_series(["bitcoin", "ethereum"], "my-bucket", days=3)

    assert isinstance(df, pd.DataFrame)
    assert "bitcoin" in df.columns
    assert "ethereum" in df.columns
    assert len(df) == 3
