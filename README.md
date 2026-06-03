# 📈 NIO Gap Reversion Engine

A quantitative trading system that identifies and exploits gap reversion opportunities in equities (e.g., NIO) using statistical analysis and machine learning-driven signal generation.

## 🎯 Overview

The NIO Gap Reversion Engine is a production-ready pipeline that detects intraday price gaps and predicts mean reversion patterns within the first 30 minutes of trading. It combines **data ingestion**, **feature engineering**, **strategy evaluation**, and **backtesting** into a unified framework.

### Key Insight
Large price gaps are often market overreactions and statistically tend to revert. This engine quantifies that behavior and generates high-probability trading signals.

---

## 🧠 Strategy Concept

Gap reversion is based on the empirical observation that:

- **Large price gaps** between session close and next open are often overreactions
- **Partial or full reversion** typically occurs within a predictable timeframe
- **Volume patterns** and **moving averages** validate signal quality

### How It Works

```
Gap Detection → Entry Logic → Exit Rules → Performance Evaluation
```

1. **Detect Gap Events** – Identify significant deviations between previous close and current open
2. **Apply Entry Logic** – Filter by gap size, volume, and trend direction
3. **Execute Exits** – Define rules for partial/full reversion or time-based exits
4. **Evaluate Performance** – Backtest against historical data and assess profitability

---

## ⚙️ Core Features

### ✅ Gap Detection
- Identifies significant price deviations between previous close and current open
- Configurable gap thresholds (% or absolute)
- Supports both gap-up and gap-down scenarios

### ✅ Mean Reversion Logic
- **Entry Conditions** based on:
  - Gap size (2-5% sweet spot)
  - Direction (long/short bias)
  - Volume confirmation (>1.2× average)
  
- **Exit Rules** including:
  - Partial/full reversion targets
  - Time-based exits
  - Stop-loss protection

### ✅ High-Performance Pipeline
- **Apache Airflow** orchestration for ETL workflows
- **PySpark** for distributed data transformation
- **dbt** for analytics-grade data modeling
- **yfinance** for real-time market data

### 📊 Data Architecture (Medallion Pattern)

```
Bronze Layer (Raw Data)
    ↓
Silver Layer (Cleaned & Transformed)
    ↓
Gold Layer (Analytics-Ready Signals)
```

---

## 📋 Pipeline Workflow

The system follows a medallion data architecture:

### Step 1: Ingest (Bronze)
```
Airflow DAG → yfinance → NIO 5-minute OHLCV data → bronze_nio_prices
```

### Step 2: Transform (Silver)
```
PySpark Job → Feature Engineering
  • 20-period moving average
  • 20-day SMA
  • Volume calculations
  → silver_nio_prices
```

### Step 3: Analyze (Gold)
```
dbt Models → Signal Generation
  • Filter gaps between 2-5%
  • Above 20-day SMA (uptrend)
  • Volume > 1.2× average
  → gap_up_signals
```

### Step 4: Orchestrate
```
Airflow DAG → Execute all steps in sequence → Production-ready signals
```

**Result:** High-probability gap-up reversion signals ready for execution

![Pipeline Architecture](https://github.com/user-attachments/assets/523256e1-3f45-4368-9996-803b2879befd)

---

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Apache Airflow 2.0+
- Apache Spark 3.0+
- PostgreSQL or compatible database
- Docker (optional, recommended)

### Installation

#### Option 1: Docker (Recommended)
```bash
docker build -t nio-gap-engine .
docker run -d -p 8080:8080 nio-gap-engine
```

#### Option 2: Local Setup
```bash
# Clone repository
git clone https://github.com/AAliasghar/nio-gap-reversion-engine.git
cd nio-gap-reversion-engine

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize Airflow
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname Admin --role Admin --email admin@example.com

# Start Airflow
airflow webserver --port 8080
airflow scheduler
```

#### Option 3: dbt Setup
```bash
cd sql/nio_strategies
dbt debug
dbt deps
dbt run
dbt test
```

---

## 📖 Usage

### Running the Full Pipeline
```bash
# Trigger Airflow DAG
airflow dags trigger nio_gap_reversion_dag

# Monitor execution
airflow dags list-runs
airflow tasks list nio_gap_reversion_dag
```

### Running Individual Components

**Fetch Data:**
```bash
python src/ingest/fetch_nio_data.py --date 2026-06-03
```

**Transform Data:**
```bash
spark-submit src/transform/spark_pipeline.py
```

**Generate Signals:**
```bash
dbt run --select gap_up_signals
```

**Backtest Strategy:**
```bash
python src/backtest/backtest_engine.py --start_date 2025-01-01 --end_date 2026-06-03
```

---

## 📂 Project Structure

```
nio-gap-reversion-engine/
├── src/
│   ├── ingest/              # Data fetching (yfinance)
│   ├── transform/           # PySpark ETL jobs
│   ├── backtest/            # Backtesting engine
│   └── config/              # Configuration files
├── sql/nio_strategies/      # dbt project
│   ├── models/
│   │   ├── bronze/          # Raw data models
│   │   ├── silver/          # Cleaned data models
│   │   └── gold/            # Analytics models
│   ├── tests/               # Data quality tests
│   └── dbt_project.yml
├── dags/                    # Airflow DAGs
├── docker/                  # Docker configuration
├── requirements.txt         # Python dependencies
├── Dockerfile               # Container image
└── README.md
```

---

## 🔧 Configuration

Edit `src/config/config.yaml` to customize:

```yaml
strategy:
  gap_threshold_lower: 2.0      # 2% minimum gap
  gap_threshold_upper: 5.0      # 5% maximum gap
  volume_multiplier: 1.2         # Volume confirmation level
  sma_period: 20                 # Simple moving average period
  lookback_days: 20              # SMA lookback window

data:
  symbol: "NIO"
  interval: "5m"                 # 5-minute candles
  source: "yfinance"

backtest:
  start_date: "2025-01-01"
  end_date: "2026-06-03"
  initial_capital: 10000
  position_size: 0.1             # Risk 10% per trade
```

---

## 📊 Key Metrics

The backtesting engine evaluates:

| Metric | Description |
|--------|-------------|
| **Win Rate** | % of profitable trades |
| **Profit Factor** | Gross profit / Gross loss |
| **Sharpe Ratio** | Risk-adjusted returns |
| **Max Drawdown** | Largest peak-to-trough decline |
| **Return on Risk** | Average profit per unit of risk |

---

## 🧪 Testing & Quality

```bash
# Run dbt tests (data quality)
dbt test

# Run Python unit tests
pytest tests/

# Check code style
flake8 src/
black --check src/
```

---

## 📈 Example Output

Successful pipeline execution generates a `gap_up_signals` table:

| timestamp | symbol | gap_pct | close | sma_20 | volume | signal |
|-----------|--------|---------|-------|--------|--------|--------|
| 2026-06-03 09:30 | NIO | 3.2% | 25.50 | 24.80 | 2.1M | BUY |
| 2026-06-03 09:35 | NIO | 3.8% | 25.75 | 24.95 | 1.9M | BUY |

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## ⚠️ Disclaimer

**This is a research/educational project.** Trading involves substantial risk of loss. Backtested results do not guarantee future performance. Always:

- Use proper risk management
- Validate on live data before deploying
- Monitor signals in real-time
- Consult with financial advisors

---

## 📝 License

This project is unlicensed. Feel free to use it for educational and research purposes.

---

## 📧 Contact & Support

**Author:** [AAliasghar](https://github.com/AAliasghar)

For issues, questions, or suggestions:
- 📌 Open an [Issue](https://github.com/AAliasghar/nio-gap-reversion-engine/issues)
- 💬 Start a [Discussion](https://github.com/AAliasghar/nio-gap-reversion-engine/discussions)

---

## 🎯 Roadmap

- [ ] Real-time signal streaming via WebSocket
- [ ] Machine learning feature engineering (gradient boosting)
- [ ] Multi-symbol support (expand beyond NIO)
- [ ] Risk management module with position sizing
- [ ] Web dashboard for signal visualization
- [ ] Paper trading integration

---

## 🔗 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [Apache Airflow](https://airflow.apache.org/)
- [PySpark API](https://spark.apache.org/docs/latest/api/python/)
- [yfinance Library](https://github.com/ranaroussi/yfinance)
- [Quantitative Trading Guide](https://en.wikipedia.org/wiki/Quantitative_trading)

---

**⭐ If you found this useful, consider starring the repository!**
