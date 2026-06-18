# ADR 003 - CoinGecko over Binance API

**Status:** Accepted

## Context

The pipeline needed a reliable crypto price data source. The two primary candidates were the CoinGecko REST API and the Binance WebSocket/REST API.

## Decision

Chose the CoinGecko free API over Binance.

## Reasons

- **No API key required:** CoinGecko's free tier requires no registration or key for basic usage, removing onboarding friction for contributors.
- **Breadth of coverage:** Covers 10,000+ cryptocurrencies and global exchanges, versus Binance's own listed assets only.
- **Clean REST API:** Simple `/simple/price` and `/coins/{id}/market_chart` endpoints with predictable JSON responses and good documentation.
- **Free historical data:** Up to 365 days of daily OHLC data available on the free tier, enabling the backfill module without paid access.

## Consequences

- Rate limited on the free tier (10–50 calls per minute), requiring `time.sleep(1)` between crypto fetches in the backfill loop.
- Not suitable for tick-by-tick order book or trade data required in high-frequency trading scenarios.
