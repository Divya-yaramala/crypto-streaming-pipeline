SELECT
    crypto_id,
    DATE(event_timestamp)       AS trade_date,
    AVG(price_usd)              AS avg_price,
    MIN(price_usd)              AS min_price,
    MAX(price_usd)              AS max_price,
    MAX(price_usd) - MIN(price_usd) AS price_volatility,
    COUNT(*)                    AS event_count,
    AVG(change_24h_pct)         AS avg_change_24h
FROM {{ ref('stg_crypto_prices') }}
GROUP BY crypto_id, DATE(event_timestamp)
ORDER BY crypto_id, trade_date DESC
