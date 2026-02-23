from __future__ import annotations
import logging
import sys
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

def _run_processing(**context) -> dict:
    from process import run_processing_pipeline
    run_ts = datetime.now(timezone.utc)
    results = run_processing_pipeline(lookback_hours=48, run_ts=run_ts)
    logger.info("Processing results: %s", results)
    context["ti"].xcom_push(key="processing_results", value=results)
    if results.get("status") == "no_data":
        logger.warning("No data available for processing")
    return results

def _load_to_postgres(**context) -> dict:
    from load_postgres import run_full_load
    run_ts = datetime.now(timezone.utc)
    results = run_full_load(run_ts=run_ts)
    logger.info("Load results: %s", results)
    context["ti"].xcom_push(key="load_results", value=results)
    return results

def _refresh_analytics_views(**context) -> dict:
    import psycopg2
    import os
    conn = psycopg2.connect(
        host=os.getenv("AQ_POSTGRES_HOST", "postgres-data"),
        port=int(os.getenv("AQ_POSTGRES_PORT", "5432")),
        dbname=os.getenv("AQ_POSTGRES_DB", "airquality"),
        user=os.getenv("AQ_POSTGRES_USER", "aquser"),
        password=os.getenv("AQ_POSTGRES_PASSWORD", "aqpassword"),
        connect_timeout=15,
    )
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM analytics.latest_readings
            WHERE (location_id, parameter, measured_at) IN (
                SELECT location_id, parameter, measured_at
                FROM (
                    SELECT location_id, parameter, measured_at,
                           ROW_NUMBER() OVER (PARTITION BY location_id, parameter ORDER BY measured_at DESC) AS rn
                    FROM analytics.latest_readings
                ) ranked WHERE rn > 1
            )
        """)
        cur.execute("""
            INSERT INTO analytics.latest_readings
                (location_id, location_name, city, country, latitude, longitude,
                 parameter, value, unit, measured_at)
            SELECT DISTINCT ON (location_id, parameter)
                location_id, location_name, city, country, latitude, longitude,
                parameter, value, unit, measured_at
            FROM raw.measurements
            WHERE measured_at >= NOW() - INTERVAL '2 hours'
            ORDER BY location_id, parameter, measured_at DESC
            ON CONFLICT (location_id, parameter) DO UPDATE SET
                value = EXCLUDED.value,
                measured_at = EXCLUDED.measured_at,
                location_name = EXCLUDED.location_name,
                city = EXCLUDED.city
        """)
    conn.commit()
    conn.close()
    return {"status": "refreshed"}

with DAG(
    dag_id="dag_02_process_and_load",
    description="Clean, aggregate, and load processed air quality data into Postgres.",
    schedule="*/25 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["processing", "postgres", "aggregation"],
) as dag:
    process = PythonOperator(
        task_id="process_parquet",
        python_callable=_run_processing,
    )
    load = PythonOperator(
        task_id="load_postgres",
        python_callable=_load_to_postgres,
    )
    refresh = PythonOperator(
        task_id="refresh_analytics",
        python_callable=_refresh_analytics_views,
    )
    process >> load >> refresh
