SELECT *
FROM (
    VALUES
        ('bitcoin',  'BTC',  'Cryptocurrency', 'Proof of Work'),
        ('ethereum', 'ETH',  'Cryptocurrency', 'Proof of Stake'),
        ('solana',   'SOL',  'Cryptocurrency', 'Proof of History'),
        ('cardano',  'ADA',  'Cryptocurrency', 'Proof of Stake'),
        ('dogecoin', 'DOGE', 'Cryptocurrency', 'Proof of Work')
) AS t(crypto_id, symbol, category, consensus_mechanism)
