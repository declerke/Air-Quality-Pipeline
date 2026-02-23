import os
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path(os.getenv("AQ_DATA_PROCESSED", "/opt/airflow/data/processed"))

DB_HOST = os.getenv("AQ_POSTGRES_HOST", "postgres-data")
DB_PORT = int(os.getenv("AQ_POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("AQ_POSTGRES_DB", "airquality")
DB_USER = os.getenv("AQ_POSTGRES_USER", "aquser")
DB_PASS = os.getenv("AQ_POSTGRES_PASSWORD", "aqpassword")

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 15})

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=15,
    )

def load_recent_processed_parquet(table_name: str, lookback_hours: int = 6) -> pd.DataFrame:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    frames = []
    table_dir = PROCESSED_DIR / table_name
    if not table_dir.exists():
        return pd.DataFrame()
    for f in table_dir.rglob("*.parquet"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
            if mtime >= cutoff:
                df = pd.read_parquet(f)
                frames.append(df)
        except Exception as exc:
            logger.warning("Could not read %s: %s", f, exc)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return combined

def upsert_dataframe(df: pd.DataFrame, schema: str, table: str, conflict_cols: list[str], conn) -> int:
    if df.empty:
        return 0
    
    df = df.sort_values(by=conflict_cols).drop_duplicates(subset=conflict_cols, keep='last')
    
    df = df.where(pd.notna(df), other=None)
    columns = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    update_cols = [c for c in columns if c not in conflict_cols]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    conflict_clause = ", ".join(conflict_cols)
    sql = (
        f"INSERT INTO {schema}.{table} ({', '.join(columns)}) "
        f"VALUES %s "
        f"ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    return len(df)

def load_raw_measurements(lookback_hours: int = 2) -> int:
    from ingest import LANDING_DIR
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    frames = []
    for f in LANDING_DIR.rglob("*.parquet"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
            if mtime >= cutoff:
                df = pd.read_parquet(f)
                df["source_file"] = str(f)
                frames.append(df)
        except Exception as exc:
            logger.warning("Could not read %s: %s", f, exc)
    if not frames:
        return 0
    combined = pd.concat(frames, ignore_index=True)
    combined["measured_at"] = pd.to_datetime(combined["measured_at"], utc=True)
    combined["ingested_at"] = pd.to_datetime(combined["ingested_at"], utc=True)
    with get_connection() as conn:
        return upsert_dataframe(combined, "raw", "measurements", 
                                ["location_id", "parameter", "measured_at"], conn)

def load_hourly_averages(lookback_hours: int = 6) -> int:
    df = load_recent_processed_parquet("hourly_averages", lookback_hours)
    if df.empty:
        return 0
    df["hour_bucket"] = pd.to_datetime(df["hour_bucket"], utc=True)
    df["computed_at"] = datetime.now(timezone.utc)
    required_cols = ["location_id", "location_name", "city", "country", "latitude", "longitude",
                     "parameter", "hour_bucket", "avg_value", "min_value", "max_value", "reading_count", "computed_at"]
    df = df[[c for c in required_cols if c in df.columns]]
    with get_connection() as conn:
        return upsert_dataframe(df, "processed", "hourly_averages",
                                ["location_id", "parameter", "hour_bucket"], conn)

def load_daily_averages(lookback_hours: int = 30) -> int:
    df = load_recent_processed_parquet("daily_averages", lookback_hours)
    if df.empty:
        return 0
    df["computed_at"] = datetime.now(timezone.utc)
    df["day_bucket"] = pd.to_datetime(df["day_bucket"]).dt.date
    required_cols = ["location_id", "location_name", "city", "country", "latitude", "longitude",
                     "parameter", "day_bucket", "avg_value", "min_value", "max_value",
                     "p95_value", "reading_count", "computed_at"]
    df = df[[c for c in required_cols if c in df.columns]]
    with get_connection() as conn:
        return upsert_dataframe(df, "processed", "daily_averages",
                                ["location_id", "parameter", "day_bucket"], conn)

def load_latest_readings(lookback_hours: int = 6) -> int:
    df = load_recent_processed_parquet("latest_readings", lookback_hours)
    if df.empty:
        return 0
    df["measured_at"] = pd.to_datetime(df["measured_at"], utc=True)
    required_cols = ["location_id", "location_name", "city", "country", "latitude", "longitude",
                     "parameter", "value", "unit", "measured_at", "aqi_category"]
    df = df[[c for c in required_cols if c in df.columns]]
    with get_connection() as conn:
        return upsert_dataframe(df, "analytics", "latest_readings",
                                ["location_id", "parameter"], conn)

def load_city_daily_summary(lookback_hours: int = 30) -> int:
    df = load_recent_processed_parquet("city_daily_summary", lookback_hours)
    if df.empty:
        return 0
    df["day_bucket"] = pd.to_datetime(df["day_bucket"]).dt.date
    df["computed_at"] = datetime.now(timezone.utc)
    required_cols = ["city", "country", "parameter", "day_bucket", "avg_value", "max_value",
                     "station_count", "aqi_score", "aqi_category", "computed_at"]
    df = df[[c for c in required_cols if c in df.columns]]
    with get_connection() as conn:
        return upsert_dataframe(df, "analytics", "city_daily_summary",
                                ["city", "parameter", "day_bucket"], conn)

def run_full_load(run_ts: Optional[datetime] = None) -> dict:
    if run_ts is None:
        run_ts = datetime.now(timezone.utc)
    results = {}
    results["raw_measurements"] = load_raw_measurements(lookback_hours=3)
    results["hourly_averages"] = load_hourly_averages(lookback_hours=8)
    results["daily_averages"] = load_daily_averages(lookback_hours=36)
    results["latest_readings"] = load_latest_readings(lookback_hours=8)
    results["city_daily_summary"] = load_city_daily_summary(lookback_hours=36)
    return results
