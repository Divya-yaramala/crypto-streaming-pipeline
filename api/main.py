import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional

import psycopg2
import uvicorn
from fastapi import Depends, FastAPI
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]

app = FastAPI(title="Crypto Pipeline API", version="1.0.0")


class CryptoPrice(BaseModel):
    crypto_id: str
    price_usd: float
    market_cap_usd: Optional[float]
    change_24h_pct: Optional[float]
    event_timestamp: str
    source: str


class CryptoAlert(BaseModel):
    crypto_id: str
    alert_type: str
    message: str
    price_usd: float
    created_at: str


class CryptoAggregation(BaseModel):
    crypto_id: str
    window_start: str
    window_end: str
    avg_price: float
    min_price: float
    max_price: float
    record_count: int


def get_db_connection() -> Generator[Any, None, None]:
    """Yield a psycopg2 connection and ensure it is closed after the request."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    try:
        yield conn
    finally:
        conn.close()


@app.get("/health")
def health() -> Dict[str, str]:
    """Return a simple health-check response with the current UTC timestamp."""
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/cryptos")
def get_cryptos() -> List[str]:
    """Return the list of tracked crypto IDs."""
    return CRYPTO_IDS


@app.get("/prices/{crypto_id}", response_model=List[CryptoPrice])
def get_prices(
    crypto_id: str, hours: int = 24, conn: Any = Depends(get_db_connection)
) -> List[CryptoPrice]:
    """Retrieve recent price records for a given crypto within the specified hour window."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT crypto_id, price_usd, market_cap_usd, change_24h_pct,
                   event_timestamp, source
            FROM crypto_prices
            WHERE crypto_id = %s AND event_timestamp >= %s
            ORDER BY event_timestamp DESC
            """,
            (crypto_id, since),
        )
        rows = cur.fetchall()
    logger.info("Fetched %d prices for %s (last %dh)", len(rows), crypto_id, hours)
    return [
        CryptoPrice(
            crypto_id=r[0],
            price_usd=float(r[1]),
            market_cap_usd=float(r[2]) if r[2] is not None else None,
            change_24h_pct=float(r[3]) if r[3] is not None else None,
            event_timestamp=r[4].isoformat() if hasattr(r[4], "isoformat") else str(r[4]),
            source=r[5],
        )
        for r in rows
    ]


@app.get("/alerts/{crypto_id}", response_model=List[CryptoAlert])
def get_alerts(
    crypto_id: str, hours: int = 24, conn: Any = Depends(get_db_connection)
) -> List[CryptoAlert]:
    """Retrieve recent alerts for a given crypto within the specified hour window."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT crypto_id, alert_type, message, price_usd, created_at
            FROM crypto_alerts
            WHERE crypto_id = %s AND created_at >= %s
            ORDER BY created_at DESC
            """,
            (crypto_id, since),
        )
        rows = cur.fetchall()
    logger.info("Fetched %d alerts for %s (last %dh)", len(rows), crypto_id, hours)
    return [
        CryptoAlert(
            crypto_id=r[0],
            alert_type=r[1],
            message=r[2],
            price_usd=float(r[3]),
            created_at=r[4].isoformat() if hasattr(r[4], "isoformat") else str(r[4]),
        )
        for r in rows
    ]


@app.get("/aggregations/{crypto_id}", response_model=List[CryptoAggregation])
def get_aggregations(
    crypto_id: str, hours: int = 1, conn: Any = Depends(get_db_connection)
) -> List[CryptoAggregation]:
    """Retrieve recent aggregation windows for a given crypto."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT crypto_id, window_start, window_end,
                   avg_price, min_price, max_price, record_count
            FROM crypto_price_aggregates
            WHERE crypto_id = %s AND window_start >= %s
            ORDER BY window_start DESC
            """,
            (crypto_id, since),
        )
        rows = cur.fetchall()
    logger.info("Fetched %d aggregations for %s (last %dh)", len(rows), crypto_id, hours)
    return [
        CryptoAggregation(
            crypto_id=r[0],
            window_start=r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1]),
            window_end=r[2].isoformat() if hasattr(r[2], "isoformat") else str(r[2]),
            avg_price=float(r[3]),
            min_price=float(r[4]),
            max_price=float(r[5]),
            record_count=int(r[6]),
        )
        for r in rows
    ]


@app.get("/summary/{crypto_id}")
def get_summary(crypto_id: str, conn: Any = Depends(get_db_connection)) -> Dict[str, Any]:
    """Return the latest price, most recent alert, and latest aggregation for a crypto."""
    since_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    since_1h = datetime.now(timezone.utc) - timedelta(hours=1)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT price_usd, event_timestamp FROM crypto_prices
            WHERE crypto_id = %s ORDER BY event_timestamp DESC LIMIT 1
            """,
            (crypto_id,),
        )
        price_row = cur.fetchone()

        cur.execute(
            """
            SELECT alert_type, message, created_at FROM crypto_alerts
            WHERE crypto_id = %s AND created_at >= %s
            ORDER BY created_at DESC LIMIT 1
            """,
            (crypto_id, since_24h),
        )
        alert_row = cur.fetchone()

        cur.execute(
            """
            SELECT avg_price, window_start FROM crypto_price_aggregates
            WHERE crypto_id = %s AND window_start >= %s
            ORDER BY window_start DESC LIMIT 1
            """,
            (crypto_id, since_1h),
        )
        agg_row = cur.fetchone()

    def _iso(val: Any) -> Optional[str]:
        """Convert a value to ISO string if it supports isoformat, else return None."""
        return val.isoformat() if val and hasattr(val, "isoformat") else None

    return {
        "crypto_id": crypto_id,
        "latest_price": {
            "price_usd": float(price_row[0]) if price_row else None,
            "timestamp": _iso(price_row[1]) if price_row else None,
        },
        "latest_alert": {
            "alert_type": alert_row[0] if alert_row else None,
            "message": alert_row[1] if alert_row else None,
            "created_at": _iso(alert_row[2]) if alert_row else None,
        },
        "recent_aggregation": {
            "avg_price": float(agg_row[0]) if agg_row else None,
            "window_start": _iso(agg_row[1]) if agg_row else None,
        },
    }


@app.get("/dashboard")
def get_dashboard(conn: Any = Depends(get_db_connection)) -> Dict[str, Any]:
    """Return the latest price and 24h change for every tracked crypto."""
    result: Dict[str, Any] = {}
    for crypto_id in CRYPTO_IDS:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT price_usd, change_24h_pct, event_timestamp
                FROM crypto_prices
                WHERE crypto_id = %s ORDER BY event_timestamp DESC LIMIT 1
                """,
                (crypto_id,),
            )
            row = cur.fetchone()
        result[crypto_id] = {
            "price_usd": float(row[0]) if row else None,
            "change_24h_pct": float(row[1]) if row and row[1] is not None else None,
            "last_updated": row[2].isoformat() if row and hasattr(row[2], "isoformat") else None,
        }
    logger.info("Dashboard data fetched for %d cryptos", len(CRYPTO_IDS))
    return result


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
