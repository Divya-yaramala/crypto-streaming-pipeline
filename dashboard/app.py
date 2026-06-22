import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import plotly.express as px
import psycopg2
import requests
import streamlit as st

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Crypto Intelligence Dashboard",
    layout="wide",
    page_icon="📊",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")

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


def get_db_connection() -> Optional[psycopg2.extensions.connection]:
    """Open a psycopg2 connection; return None if the database is unreachable."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
        )
        return conn
    except Exception as e:
        logger.error("Failed to connect to PostgreSQL: %s", e)
        return None


def load_crypto_prices(
    conn: Optional[psycopg2.extensions.connection], hours: int = 24
) -> pd.DataFrame:
    """Query crypto_prices for recent rows and return a DataFrame; empty on error."""
    if conn is None:
        return pd.DataFrame()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        query = """
            SELECT crypto_id, price_usd, change_24h_pct, event_timestamp
            FROM crypto_prices
            WHERE event_timestamp >= %s
            ORDER BY event_timestamp DESC
        """
        return pd.read_sql_query(query, conn, params=(cutoff,))
    except Exception as e:
        logger.error("Failed to load crypto prices: %s", e)
        return pd.DataFrame()


def load_crypto_alerts(
    conn: Optional[psycopg2.extensions.connection], hours: int = 24
) -> pd.DataFrame:
    """Query crypto_alerts for recent rows and return a DataFrame; empty on error."""
    if conn is None:
        return pd.DataFrame()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        query = """
            SELECT alert_type, crypto_id, message, created_at
            FROM crypto_alerts
            WHERE created_at >= %s
            ORDER BY created_at DESC
            LIMIT 50
        """
        return pd.read_sql_query(query, conn, params=(cutoff,))
    except Exception as e:
        logger.error("Failed to load crypto alerts: %s", e)
        return pd.DataFrame()


def load_aggregations(
    conn: Optional[psycopg2.extensions.connection], hours: int = 1
) -> pd.DataFrame:
    """Query crypto_price_aggregates for recent windows; return empty DataFrame on error."""
    if conn is None:
        return pd.DataFrame()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        query = """
            SELECT crypto_id, window_start, window_end,
                   avg_price, min_price, max_price, price_range, record_count
            FROM crypto_price_aggregates
            WHERE window_start >= %s
            ORDER BY window_start DESC
            LIMIT 50
        """
        return pd.read_sql_query(query, conn, params=(cutoff,))
    except Exception as e:
        logger.error("Failed to load aggregations: %s", e)
        return pd.DataFrame()


def fetch_live_prices() -> dict:
    """Fetch live USD prices for all tracked cryptos from CoinGecko; return {} on error."""
    try:
        ids_param = ",".join(CRYPTO_IDS)
        url = (
            f"{COINGECKO_BASE_URL}/simple/price"
            f"?ids={ids_param}&vs_currencies=usd"
            f"&include_24hr_change=true&include_market_cap=true"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("Failed to fetch live prices: %s", e)
        return {}


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Controls")
    auto_refresh = st.toggle("Auto-refresh (60s)", value=False)
    st.divider()

    st.subheader("Date Range")
    hours = st.slider("Hours of history", min_value=1, max_value=168, value=24, step=1)
    cutoff_str = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M")
    st.caption(f"Showing data from {cutoff_str} UTC")
    st.divider()

    st.subheader("Cryptos")
    selected_cryptos = [
        cid for cid in CRYPTO_IDS if st.checkbox(CRYPTO_SYMBOLS[cid], value=True, key=f"cb_{cid}")
    ]
    if not selected_cryptos:
        selected_cryptos = CRYPTO_IDS[:]

# ── Data loading ──────────────────────────────────────────────────────────────
conn = get_db_connection()
live_prices = fetch_live_prices()
prices_df = load_crypto_prices(conn, hours=hours)
alerts_df = load_crypto_alerts(conn, hours=hours)
agg_df = load_aggregations(conn, hours=1)
if conn:
    conn.close()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 Crypto Intelligence Dashboard")
st.caption("Real-time prices powered by Kafka + PySpark")
st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
st.divider()

# ── Row 1: KPI Metrics ────────────────────────────────────────────────────────
kpi_cols = st.columns(5)
for i, cid in enumerate(CRYPTO_IDS):
    with kpi_cols[i]:
        symbol = CRYPTO_SYMBOLS[cid]
        if live_prices and cid in live_prices:
            price = live_prices[cid].get("usd") or 0
            change = live_prices[cid].get("usd_24h_change") or 0
            price_str = f"${price:,.2f}" if price >= 1 else f"${price:.4f}"
            st.metric(label=symbol, value=price_str, delta=f"{change:+.2f}%")
        else:
            st.metric(label=symbol, value="N/A", delta=None)

st.divider()

# ── Row 2: Price Charts ───────────────────────────────────────────────────────
col_btc, col_eth = st.columns(2)

with col_btc:
    st.subheader("₿ Bitcoin (BTC) — Price History")
    if not prices_df.empty and "bitcoin" in prices_df["crypto_id"].values:
        btc_df = prices_df[prices_df["crypto_id"] == "bitcoin"].sort_values("event_timestamp")
        fig = px.line(
            btc_df,
            x="event_timestamp",
            y="price_usd",
            color_discrete_sequence=[CRYPTO_COLORS["bitcoin"]],
        )
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Price (USD)",
            showlegend=False,
            height=300,
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No BTC price history yet. Start the Kafka producer to collect data.")

with col_eth:
    st.subheader("Ξ Ethereum (ETH) — Price History")
    if not prices_df.empty and "ethereum" in prices_df["crypto_id"].values:
        eth_df = prices_df[prices_df["crypto_id"] == "ethereum"].sort_values("event_timestamp")
        fig = px.line(
            eth_df,
            x="event_timestamp",
            y="price_usd",
            color_discrete_sequence=[CRYPTO_COLORS["ethereum"]],
        )
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Price (USD)",
            showlegend=False,
            height=300,
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No ETH price history yet. Start the Kafka producer to collect data.")

st.divider()

# ── Row 3: Alerts and Aggregations ───────────────────────────────────────────
col_alerts, col_agg = st.columns(2)

with col_alerts:
    st.subheader("🚨 Recent PUMP / DUMP Alerts")
    if not alerts_df.empty:
        st.dataframe(alerts_df, use_container_width=True, hide_index=True)
    else:
        st.info("No alerts triggered in the selected time window.")

with col_agg:
    st.subheader("📈 1-Minute Aggregations")
    if not agg_df.empty:
        display_cols = [
            "crypto_id",
            "window_start",
            "avg_price",
            "min_price",
            "max_price",
            "record_count",
        ]
        available = [c for c in display_cols if c in agg_df.columns]
        st.dataframe(agg_df[available], use_container_width=True, hide_index=True)
    else:
        st.info("No aggregation data yet. Start the PySpark processor to compute windows.")

st.divider()

# ── Row 4: All Cryptos Comparison ─────────────────────────────────────────────
st.subheader("💱 Current Price Comparison — All Cryptos")
if live_prices:
    chart_rows = [
        {
            "Crypto": CRYPTO_SYMBOLS[cid],
            "Price (USD)": live_prices[cid].get("usd", 0),
        }
        for cid in selected_cryptos
        if cid in live_prices
    ]
    if chart_rows:
        comp_df = pd.DataFrame(chart_rows)
        color_map = {CRYPTO_SYMBOLS[cid]: CRYPTO_COLORS[cid] for cid in selected_cryptos}
        fig = px.bar(
            comp_df,
            x="Crypto",
            y="Price (USD)",
            color="Crypto",
            color_discrete_map=color_map,
            text_auto=".2s",
        )
        fig.update_layout(
            showlegend=False,
            height=350,
            margin=dict(l=0, r=0, t=20, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Live price data unavailable. Check your network connection.")

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(60)
    st.rerun()
