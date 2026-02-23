from __future__ import annotations
import logging
import sys
import os
import time
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

def _ingest_kenya(**context) -> dict:
    from ingest import ingest_country
    run_ts = datetime.now(timezone.utc)
    result = ingest_country("KE", run_ts=run_ts)
    logger.info("Kenya ingestion result: %s", result)
    context["ti"].xcom_push(key="kenya_result", value=result)
    return result

def _ingest_global(**context) -> dict:
    from ingest import ingest_country
    run_ts = datetime.now(timezone.utc)
    
    global_country_codes = ["GB", "US", "CN", "IN", "NG"]
    
    results = []
    for code in global_country_codes:
        result = ingest_country(code, run_ts=run_ts)
        results.append(result)
        time.sleep(1.0)
        
    total = sum(r.get("records", 0) for r in results)
    logger.info("Global ingestion results: total=%d", total)
    context["ti"].xcom_push(key="global_results", value=results)
    return {"status": "success", "total_records": total, "countries": len(results)}

with DAG(
    dag_id="dag_01_ingest_raw",
    description="Poll Air Quality API for Kenya and global reference cities; land raw Parquet files.",
    schedule="*/20 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "meteo", "kenya"],
) as dag:

    ingest_kenya = PythonOperator(
        task_id="ingest_kenya",
        python_callable=_ingest_kenya,
    )

    ingest_global = PythonOperator(
        task_id="ingest_global",
        python_callable=_ingest_global,
    )

    ingest_kenya >> ingest_global