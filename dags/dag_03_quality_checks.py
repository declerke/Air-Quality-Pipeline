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
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

def _run_freshness_check(**context) -> dict:
    from quality import get_connection, check_freshness
    run_id = context["run_id"]
    with get_connection() as conn:
        result = check_freshness(conn, run_id=run_id, dag_id="dag_03_quality_checks")
    logger.info("Freshness: %s", result)
    if result["status"] == "failed":
        raise ValueError(f"Freshness check FAILED: {result['details']}")
    return result

def _run_completeness_check(**context) -> dict:
    from quality import get_connection, check_completeness
    run_id = context["run_id"]
    with get_connection() as conn:
        result = check_completeness(conn, run_id=run_id, dag_id="dag_03_quality_checks")
    logger.info("Completeness: %s", result)
    if result["status"] == "failed":
        raise ValueError(f"Completeness check FAILED: {result['details']}")
    return result

def _run_range_check(**context) -> dict:
    from quality import get_connection, check_value_ranges
    run_id = context["run_id"]
    with get_connection() as conn:
        result = check_value_ranges(conn, run_id=run_id, dag_id="dag_03_quality_checks")
    logger.info("Range: %s", result)
    if result["status"] == "failed":
        raise ValueError(f"Range check FAILED: {result['details']}")
    return result

def _run_duplicate_check(**context) -> dict:
    from quality import get_connection, check_duplicates
    run_id = context["run_id"]
    with get_connection() as conn:
        result = check_duplicates(conn, run_id=run_id, dag_id="dag_03_quality_checks")
    logger.info("Duplicates: %s", result)
    if result["status"] == "failed":
        raise ValueError(f"Duplicate check FAILED: {result['details']}")
    return result

def _run_coverage_check(**context) -> dict:
    from quality import get_connection, check_location_coverage
    run_id = context["run_id"]
    with get_connection() as conn:
        result = check_location_coverage(conn, run_id=run_id, dag_id="dag_03_quality_checks")
    logger.info("Coverage: %s", result)
    return result

def _quality_summary(**context) -> dict:
    ti = context["ti"]
    tasks = ["freshness", "completeness", "range", "duplicates", "coverage"]
    results = {}
    for t in tasks:
        try:
            results[t] = ti.xcom_pull(task_ids=f"check_{t}", key="return_value")
        except Exception:
            results[t] = None
    passed = [k for k, v in results.items() if v and v.get("status") in ("passed", "warning")]
    failed = [k for k, v in results.items() if v and v.get("status") == "failed"]
    summary = {
        "passed_checks": passed,
        "failed_checks": failed,
        "total_checks": len(tasks),
        "run_id": context["run_id"],
    }
    logger.info("Quality summary: %s", summary)
    return summary

with DAG(
    dag_id="dag_03_quality_checks",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["quality", "validation", "monitoring"],
) as dag:
    check_freshness_task = PythonOperator(
        task_id="check_freshness",
        python_callable=_run_freshness_check,
    )
    check_completeness_task = PythonOperator(
        task_id="check_completeness",
        python_callable=_run_completeness_check,
    )
    check_range_task = PythonOperator(
        task_id="check_range",
        python_callable=_run_range_check,
    )
    check_duplicates_task = PythonOperator(
        task_id="check_duplicates",
        python_callable=_run_duplicate_check,
    )
    check_coverage_task = PythonOperator(
        task_id="check_coverage",
        python_callable=_run_coverage_check,
    )
    summary_task = PythonOperator(
        task_id="quality_summary",
        python_callable=_quality_summary,
        trigger_rule="all_done",
    )
    [check_freshness_task, check_completeness_task, check_range_task,
     check_duplicates_task, check_coverage_task] >> summary_task