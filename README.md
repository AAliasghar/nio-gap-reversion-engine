# nio-gap-reversion-engine
This engine identifies intraday gap deviations and calculates Volume-Weighted metrics to predict mean reversion in the first 30 minutes of trading

Step‑by‑step:

Ingest – Airflow triggers yfinance fetch → bronze_nio_prices

Transform – PySpark computes 20‑period & 20‑day moving averages → silver_nio_prices

Analyze – dbt model filters gaps between 2‑5%, above 20‑day SMA, with volume >1.2× avg → gap_up_signals

Orchestrate – Airflow DAG runs all steps in sequence

Result: A table of high‑probability gap‑up signals for mean‑reversion trading.

This pipeline ingests NIO 5‑minute data, transforms it, and applies a gap‑up reversion strategy
<img width="4148" height="582" alt="nio-gap-reversion-engine" src="https://github.com/user-attachments/assets/523256e1-3f45-4368-9996-803b2879befd" />
