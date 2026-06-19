# ADR 008 - S3 Caching Strategy

## Status
Accepted

## Context
Needed caching for expensive CoinGecko API calls to reduce latency and avoid rate limiting during high-frequency polling.

## Decision
Built S3-based caching with TTL expiry stored under the `cache/crypto/` prefix.

## Reasons
- S3 already used for all storage — no additional infrastructure needed
- TTL prevents stale data from being served
- Persistent across restarts, unlike in-memory caches
- Cache keys derived from MD5 hash of function name + parameters for consistency

## Consequences
- Higher latency than Redis (~50ms vs ~1ms) — not suitable for sub-second caching
- Cache reads add one S3 GET per lookup; cache writes add one S3 PUT
- Expired entries must be cleared manually or via `clear_expired_cache()`
