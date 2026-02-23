import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)

DB_HOST = os.getenv("AQ_POSTGRES_HOST", "postgres-data")
DB_PORT = int(os.getenv("AQ_POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("AQ_POSTGRES_DB", "airquality")
DB_USER = os.getenv("AQ_POSTGRES_USER", "aquser")
DB_PASS = os.getenv("AQ_POSTGRES_PASSWORD", "aqpassword")

FRESHNESS_THRESHOLD_HOURS = int(os.getenv("FRESHNESS_THRESHOLD_HOURS", "3"))
MIN_EXPECTED_LOCATIONS = int(os.getenv("MIN_EXPECTED_LOCATIONS", "1"))
MAX_NULL_RATE = float(os.getenv("MAX_NULL_RATE", "0.30"))
MAX_DUPLICATE_RATE = float(os.getenv("MAX_DUPLICATE_RATE", "0.05"))

VALID_RANGES = {
    "pm25": (0.0, 1000.0),
    "pm10": (0.0, 2000.0),
    "no2": (0.0, 2000.0),
    "o3": (0.0, 1000.0),
    "co": (0.0, 100000.0),
    "so2": (0.0, 2000.0),
}


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=15,
    )


def log_quality_result(run_id: str, dag_id: str, check_name: str, status: str,
                        records_checked: int, records_failed: int, details: dict, conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO processed.data_quality_log
                (run_id, dag_id, check_name, status, records_checked, records_failed, details, checked_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, dag_id, check_name, status, records_checked, records_failed,
             json.dumps(details), datetime.now(timezone.utc)),
        )
    conn.commit()


def check_freshness(conn, run_id: str, dag_id: str) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_THRESHOLD_HOURS)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS total,
                   COUNT(CASE WHEN measured_at >= %s THEN 1 END) AS fresh
            FROM raw.measurements
            """,
            (cutoff,),
        )
        row = cur.fetchone()

    total = row[0] if row else 0
    fresh = row[1] if row else 0

    status = "passed" if fresh > 0 else "failed"
    details = {
        "total_records": total,
        "fresh_records": fresh,
        "threshold_hours": FRESHNESS_THRESHOLD_HOURS,
        "cutoff": cutoff.isoformat(),
    }

    log_quality_result(run_id, dag_id, "freshness_check", status, total, total - fresh, details, conn)
    logger.info("Freshness check: %s (%d fresh / %d total)", status, fresh, total)
    return {"check": "freshness", "status": status, "details": details}


def check_completeness(conn, run_id: str, dag_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                parameter,
                COUNT(*) AS total,
                COUNT(CASE WHEN value IS NULL THEN 1 END) AS null_count
            FROM raw.measurements
            WHERE measured_at >= NOW() - INTERVAL '24 hours'
            GROUP BY parameter
            """
        )
        rows = cur.fetchall()

    failed_params = []
    total_records = 0
    total_nulls = 0

    for param, total, null_count in rows:
        total_records += total
        total_nulls += null_count
        null_rate = null_count / total if total > 0 else 0
        if null_rate > MAX_NULL_RATE:
            failed_params.append({"parameter": param, "null_rate": round(null_rate, 4)})

    status = "failed" if failed_params else "passed"
    details = {
        "total_records_24h": total_records,
        "total_nulls": total_nulls,
        "failed_parameters": failed_params,
        "max_null_rate": MAX_NULL_RATE,
    }

    log_quality_result(run_id, dag_id, "completeness_check", status, total_records, total_nulls, details, conn)
    logger.info("Completeness check: %s (%d failed params)", status, len(failed_params))
    return {"check": "completeness", "status": status, "details": details}


def check_value_ranges(conn, run_id: str, dag_id: str) -> dict:
    violations = []
    total_checked = 0
    total_violated = 0

    with conn.cursor() as cur:
        for param, (lo, hi) in VALID_RANGES.items():
            cur.execute(
                """
                SELECT COUNT(*) AS total,
                       COUNT(CASE WHEN value < %s OR value > %s THEN 1 END) AS out_of_range
                FROM raw.measurements
                WHERE parameter = %s
                  AND measured_at >= NOW() - INTERVAL '24 hours'
                """,
                (lo, hi, param),
            )
            row = cur.fetchone()
            if row:
                total = row[0]
                oor = row[1]
                total_checked += total
                total_violated += oor
                if oor > 0:
                    violations.append({"parameter": param, "out_of_range": oor, "total": total})

    status = "failed" if total_violated > total_checked * 0.01 else "passed"
    details = {
        "total_checked": total_checked,
        "total_violated": total_violated,
        "violations_by_parameter": violations,
    }

    log_quality_result(run_id, dag_id, "range_check", status, total_checked, total_violated, details, conn)
    logger.info("Range check: %s (%d violations)", status, total_violated)
    return {"check": "range", "status": status, "details": details}


def check_duplicates(conn, run_id: str, dag_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS total_rows,
                   COUNT(*) - COUNT(DISTINCT (location_id, parameter, measured_at)) AS dup_count
            FROM raw.measurements
            WHERE measured_at >= NOW() - INTERVAL '24 hours'
            """
        )
        row = cur.fetchone()

    total = row[0] if row else 0
    dups = row[1] if row else 0
    dup_rate = dups / total if total > 0 else 0

    status = "failed" if dup_rate > MAX_DUPLICATE_RATE else "passed"
    details = {
        "total_records": total,
        "duplicate_count": dups,
        "duplicate_rate": round(dup_rate, 6),
        "max_allowed_rate": MAX_DUPLICATE_RATE,
    }

    log_quality_result(run_id, dag_id, "duplicate_check", status, total, dups, details, conn)
    logger.info("Duplicate check: %s (rate=%.4f)", status, dup_rate)
    return {"check": "duplicates", "status": status, "details": details}


def check_location_coverage(conn, run_id: str, dag_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT country, COUNT(DISTINCT location_id) AS location_count
            FROM raw.measurements
            WHERE measured_at >= NOW() - INTERVAL '24 hours'
            GROUP BY country
            ORDER BY location_count DESC
            """
        )
        rows = cur.fetchall()

    coverage = [{"country": r[0], "locations": r[1]} for r in rows]
    kenya_locations = next((r["locations"] for r in coverage if r["country"] == "KE"), 0)

    status = "warning" if kenya_locations < MIN_EXPECTED_LOCATIONS else "passed"
    details = {
        "coverage_by_country": coverage,
        "kenya_locations_24h": kenya_locations,
        "min_expected": MIN_EXPECTED_LOCATIONS,
    }

    log_quality_result(run_id, dag_id, "coverage_check", status, sum(r["locations"] for r in coverage),
                       0, details, conn)
    logger.info("Coverage check: %s (Kenya stations: %d)", status, kenya_locations)
    return {"check": "coverage", "status": status, "details": details}


def run_all_checks(run_id: Optional[str] = None, dag_id: str = "quality_dag") -> dict:
    if run_id is None:
        run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")

    results = []
    critical_failures = []

    with get_connection() as conn:
        for check_fn in [check_freshness, check_completeness, check_value_ranges,
                          check_duplicates, check_location_coverage]:
            try:
                result = check_fn(conn, run_id, dag_id)
                results.append(result)
                if result["status"] == "failed":
                    critical_failures.append(result["check"])
            except Exception as exc:
                logger.error("Quality check %s raised exception: %s", check_fn.__name__, exc)
                results.append({"check": check_fn.__name__, "status": "error", "error": str(exc)})
                critical_failures.append(check_fn.__name__)

    overall_status = "failed" if critical_failures else "passed"
    summary = {
        "run_id": run_id,
        "overall_status": overall_status,
        "critical_failures": critical_failures,
        "checks": results,
    }

    logger.info("Quality checks complete. Overall: %s. Failures: %s", overall_status, critical_failures)

    if critical_failures:
        failure_str = ", ".join(critical_failures)
        raise ValueError(f"Data quality FAILED on checks: {failure_str}. Run ID: {run_id}")

    return summary