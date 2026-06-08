import os

CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]
CRYPTO_SYMBOLS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "cardano": "ADA",
    "dogecoin": "DOGE",
}
CRYPTO_COLORS = {
    "bitcoin": "#F7931A",
    "ethereum": "#627EEA",
    "solana": "#9945FF",
    "cardano": "#0033AD",
    "dogecoin": "#C2A633",
}
REFRESH_INTERVAL = 60
DEFAULT_HOURS = 24
COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
