"""
Airflow DAG: fetches daily stock data and updates the PostgreSQL
stock_prices table on a scheduled basis.

The actual fetch/parse/store logic lives in scripts/fetch_stock_data.py
(mounted into the container alongside the dags/ folder) so it can be
unit-tested and run standalone outside of Airflow too.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append("/opt/airflow/scripts")
from fetch_stock_data import run_pipeline  # noqa: E402


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}


def run_stock_pipeline(**context):
    """Task callable: runs the full fetch -> parse -> upsert cycle."""
    results = run_pipeline()
    failures = {sym: status for sym, status in results.items() if status not in ("SUCCESS",)}
    if failures and len(failures) == len(results):
        # Every symbol failed this run -- surface it as a task failure so
        # Airflow's retry/alerting kicks in. Partial failures (some symbols
        # ok, some not) are logged but don't fail the whole DAG run, since
        # one bad ticker shouldn't block the rest of the data landing.
        raise RuntimeError(f"All symbols failed this run: {failures}")
    return results


with DAG(
    dag_id="stock_market_pipeline",
    description="Fetch stock market data hourly and upsert into PostgreSQL",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock-data", "assignment"],
) as dag:

    fetch_and_store = PythonOperator(
        task_id="fetch_parse_store_stock_data",
        python_callable=run_stock_pipeline,
    )

    fetch_and_store
