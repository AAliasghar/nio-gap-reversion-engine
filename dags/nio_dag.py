import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.scanner_nio import scan_for_signals

# 1. Point Airflow to your project directory so it can find ingest_nio.py
sys.path.insert(
    0, "/opt/airflow/scripts"
)  # This should match the volume mapping in docker-compose.yml
from ingest_nio import NIODataPipeline


# 2. Define the "Worker" function
def run_nio_ingestion():
    CONN = "postgresql://quant_user:quant_password@nio_postgres:5432/trading_warehouse"
    # Note: Inside Docker, 'localhost' might need to be 'host.docker.internal'
    # or the name of your DB service (e.g., 'postgres')

    pipeline = NIODataPipeline("NIO", CONN)

    # Run the logic we built together
    last_ts = pipeline.get_last_timestamp()
    raw_data = pipeline.fetch_data()
    pipeline.transform_and_load(raw_data, last_ts)


# 2. Add a second worker function
def run_nio_scanner():
    # This calls the logic we wrote for the 69% win-rate strategy
    from scripts.scanner_nio import scan_for_signals

    scan_for_signals()


# 3. Define the Schedule and Settings
default_args = {
    "owner": "aliasghar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "nio_market_ingestion",
    default_args=default_args,
    description="Fetches NIO 5m data every weekday for Gap Trading",
    schedule_interval="30 13 * * 1-5",  # 13:30 UTC is 9:30 AM EST (usually)
    start_date=datetime(2026, 3, 1),
    catchup=False,
) as dag:

    # 4. Define the Task
    ingest_task = PythonOperator(
        task_id="fetch_and_load_nio",
        python_callable=run_nio_ingestion,
    )

    # NEW TASK
    scanner_task = PythonOperator(
        task_id="analyze_gap_signals",
        python_callable=run_nio_scanner,
    )

    # 5. SET THE DEPENDENCY SO THE SCANNER RUNS AFTER INGESTION
    ingest_task >> scanner_task
