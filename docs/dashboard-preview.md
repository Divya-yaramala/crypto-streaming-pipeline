# Dashboard Preview

## Live Dashboard Features

### KPI Row
Shows real-time prices for all 5 cryptos with 24h change indicators:
- 🟢 Green = price up
- 🔴 Red = price down

### Price Charts
- BTC/USD 24-hour line chart
- ETH/USD 24-hour line chart
- Built with Plotly for interactive zoom/pan

### Alerts Section
- PUMP alerts: price up > 10% in 24h
- DUMP alerts: price down > 10% in 24h
- Real-time from PostgreSQL

### How to Run
streamlit run dashboard/app.py
Open: http://localhost:8501
