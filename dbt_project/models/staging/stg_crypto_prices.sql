SELECT
    crypto_id,
    price_usd,
    market_cap_usd,
    change_24h_pct,
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
    source,
    ingested_at,
    price_usd - LAG(price_usd) OVER (
        PARTITION BY crypto_id ORDER BY event_timestamp
    ) AS price_change_usd,
    (price_usd - LAG(price_usd) OVER (
        PARTITION BY crypto_id ORDER BY event_timestamp
    )) > 0 AS is_price_increase
FROM CRYPTO_PIPELINE_DB.RAW.CRYPTO_PRICES
ORDER BY crypto_id, event_timestamp
