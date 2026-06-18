# ADR 006 - Streamlit over Plotly Dash

**Status:** Accepted

## Context

The pipeline needed a real-time dashboard to visualise live crypto prices, 24-hour charts, PUMP/DUMP alerts, and 1-minute aggregations.

## Decision

Chose Streamlit over Plotly Dash.

## Reasons

- **Pure Python:** Streamlit requires no HTML, CSS, or JavaScript — the entire dashboard is written in Python, consistent with the rest of the codebase.
- **Built-in auto-refresh:** `st.rerun()` and `time.sleep()` provide simple polling without WebSocket infrastructure.
- **Development speed:** A working dashboard prototype was built in a single session; Dash requires callback wiring that adds boilerplate.
- **Free cloud deployment:** Streamlit Community Cloud deploys directly from a GitHub repo at no cost.

## Consequences

- Less customisable than Dash — custom CSS and component behaviour require workarounds or third-party component libraries.
- Single-page application only; multi-page navigation requires `st.navigation` which is less mature than Dash's routing.
