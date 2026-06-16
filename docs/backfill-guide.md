# Backfill Guide — Crypto Streaming Pipeline

## When to Use Backfill
- Pipeline was down for several days
- New crypto added to tracking list
- Historical data needed for ML training
- Data corruption detected and fixed

## Commands

### Basic backfill (last 30 days, all cryptos)
python scripts/run_backfill.py --days 30

### Single crypto backfill
python scripts/run_backfill.py --crypto bitcoin --days 90

### Preview without loading
python scripts/run_backfill.py --days 30 --dry-run

### S3 only (skip PostgreSQL)
python scripts/run_backfill.py --days 30 --s3-only

### Maximum historical data (1 year)
python scripts/run_backfill.py --days 365

## Data Sources
- CoinGecko free API: up to 365 days of daily OHLC data
- No API key required for basic historical data
- Rate limit: 10-50 calls per minute on free tier

## S3 Structure After Backfill
raw/crypto/YYYY/MM/DD/bitcoin/historical_timestamp.json
raw/crypto/YYYY/MM/DD/ethereum/historical_timestamp.json

## PostgreSQL After Backfill
All records inserted into crypto_prices table
ON CONFLICT DO NOTHING ensures no duplicates
