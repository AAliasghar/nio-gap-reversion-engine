import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
from pathlib import Path


# 1. FIRST: Point Airflow to your project directory so it can find ingest_nio.py
# This MUST happen before importing your custom scripts
sys.path.append("/opt/airflow/scripts")


# 2. SECOND:  import your custom logic
# Python can now find these because we gave it the "map" above
from transform_nio_spark import run_spark_transform
from ingest_nio import NIODataPipeline

# This calls the logic we wrote for the 69% win-rate strategy
from scanner_nio import scan_for_signals


# 3. Timezone definition
local_tz = pendulum.timezone("Europe/Berlin")


# 4. Define the "Worker" function
def run_nio_ingestion():
    CONN = "postgresql://quant_user:quant_password@nio_postgres:5432/trading_warehouse"
    # Note: Inside Docker, 'localhost' might need to be 'host.docker.internal'
    # or the name of your DB service (e.g., 'postgres')

    pipeline = NIODataPipeline("NIO", CONN)

    # Run the logic we built together
    last_ts = pipeline.get_last_timestamp()
    raw_data = pipeline.fetch_data()
    pipeline.transform_and_load(raw_data, last_ts)


# 4. Add a second worker function
def run_nio_scanner():

    scan_for_signals()


# 5. Define the Schedule and Settings
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
    schedule_interval="30 9 * * 1-5",  # 09:30 Germany time, Monday‑Friday
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),  # TZ here`
    catchup=False,
) as dag:

    # 6. Define the Task
    ingest_task = PythonOperator(
        task_id="fetch_and_load_nio",
        python_callable=run_nio_ingestion,
    )

    # Spark Transform TASK
    spark_transform_task = PythonOperator(
        task_id="pyspark_silver_transform",
        python_callable=run_spark_transform,
    )

    # Scanner TASK
    scanner_task = PythonOperator(
        task_id="analyze_gap_signals",
        python_callable=run_nio_scanner,
    )

    # 7. SET THE DEPENDENCY SO THE SCANNER RUNS AFTER INGESTION
    ingest_task >> spark_transform_task >> scanner_task
