SELECT
    crypto_id,
    alert_type,
    message,
    price_usd,
    created_at,
    CASE
        WHEN alert_type = 'PUMP' THEN 'HIGH'
        WHEN alert_type = 'DUMP' THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM CRYPTO_PIPELINE_DB.RAW.CRYPTO_ALERTS
ORDER BY created_at DESC
