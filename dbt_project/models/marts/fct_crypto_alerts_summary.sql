SELECT
    crypto_id,
    DATE(created_at)                            AS alert_date,
    COUNT(*)                                    AS total_alerts,
    COUNT(CASE WHEN alert_type = 'PUMP' THEN 1 END) AS pump_count,
    COUNT(CASE WHEN alert_type = 'DUMP' THEN 1 END) AS dump_count,
    COUNT(*) / 24.0                             AS alert_rate
FROM {{ ref('stg_crypto_alerts') }}
GROUP BY crypto_id, DATE(created_at)
ORDER BY crypto_id, alert_date DESC
