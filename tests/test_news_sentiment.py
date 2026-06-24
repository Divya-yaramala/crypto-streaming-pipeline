import os
from unittest.mock import MagicMock, patch

from consumer.news_sentiment import analyze_sentiment, fetch_crypto_news, save_sentiment_to_s3


def test_analyze_sentiment_bullish():
    headlines = [
        "Bitcoin surges to new record high",
        "Ethereum bulls push price up with massive gains",
        "Crypto market pumps as buyers rush in",
        "BTC moon incoming as bullish trend continues",
    ]
    result = analyze_sentiment(headlines, "bitcoin")
    assert result["sentiment_label"] == "BULLISH"
    assert result["positive_count"] > result["negative_count"]


def test_analyze_sentiment_bearish():
    headlines = [
        "Bitcoin crashes after major hack revealed",
        "Ethereum dumps as ban fears spread",
        "Crypto market falls on heavy sell pressure",
        "Bears take control as prices drop sharply",
        "Market crashes further in bearish downturn",
    ]
    result = analyze_sentiment(headlines, "ethereum")
    assert result["sentiment_label"] == "BEARISH"
    assert result["negative_count"] > result["positive_count"]


def test_analyze_sentiment_neutral():
    headlines = [
        "Bitcoin surges on positive news",
        "Ethereum crashes amid uncertainty",
        "Solana trading sideways today",
        "Analysts divided on crypto direction",
    ]
    result = analyze_sentiment(headlines, "solana")
    assert result["sentiment_label"] == "NEUTRAL"
    assert "sentiment_score" in result
    assert -20 <= result["sentiment_score"] <= 20


def test_save_sentiment_to_s3_success():
    sentiment = {
        "crypto_id": "bitcoin",
        "sentiment_label": "BULLISH",
        "sentiment_score": 40.0,
    }
    mock_s3 = MagicMock()
    with patch("consumer.news_sentiment.boto3.client", return_value=mock_s3):
        result = save_sentiment_to_s3(sentiment, "bitcoin", "my-bucket", "2026/06/24")
    assert result is True
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert "processed/sentiment/crypto/2026/06/24/bitcoin.json" == call_kwargs["Key"]


def test_fetch_crypto_news_no_api_key():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [
        {"title": "Bitcoin surges to new highs"},
        {"title": "Crypto market gains momentum"},
    ]
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("NEWS_API_KEY", None)
        with patch("consumer.news_sentiment.NEWS_API_KEY", None):
            with patch("consumer.news_sentiment.requests.get", return_value=mock_response):
                result = fetch_crypto_news("bitcoin")
    assert isinstance(result, list)
    assert len(result) == 2
