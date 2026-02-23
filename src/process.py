import os
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

LANDING_DIR = Path(os.getenv("AQ_DATA_LANDING", "/opt/airflow/data/landing"))
PROCESSED_DIR = Path(os.getenv("AQ_DATA_PROCESSED", "/opt/airflow/data/processed"))
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

VALID_RANGES = {
    "pm25": (0.0, 1000.0),
    "pm10": (0.0, 2000.0),
    "no2":  (0.0, 2000.0),
    "o3":   (0.0, 1000.0),
    "co":   (0.0, 100000.0),
    "so2":  (0.0, 2000.0),
}

AQI_BREAKPOINTS_PM25 = [
    (0.0, 12.0, 0, 50, "Good"),
    (12.1, 35.4, 51, 100, "Moderate"),
    (35.5, 55.4, 101, 150, "Unhealthy for Sensitive Groups"),
    (55.5, 150.4, 151, 200, "Unhealthy"),
    (150.5, 250.4, 201, 300, "Very Unhealthy"),
    (250.5, 500.4, 301, 500, "Hazardous"),
]

def compute_aqi_pm25(concentration: float) -> tuple[float, str]:
    for bp_lo, bp_hi, i_lo, i_hi, category in AQI_BREAKPOINTS_PM25:
        if bp_lo <= concentration <= bp_hi:
            aqi = ((i_hi - i_lo) / (bp_hi - bp_lo)) * (concentration - bp_lo) + i_lo
            return round(aqi, 1), category
    if concentration > 500.4:
        return 500.0, "Hazardous"
    return 0.0, "Good"

def load_landing_parquet(lookback_hours: int = 48) -> pd.DataFrame:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    frames = []
    for parquet_file in LANDING_DIR.rglob("*.parquet"):
        try:
            df = pd.read_parquet(parquet_file)
            frames.append(df)
        except Exception as exc:
            logger.warning("Could not read %s: %s", parquet_file, exc)
    if not frames:
        logger.warning("No parquet files found in landing dir %s", LANDING_DIR)
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    combined["measured_at"] = pd.to_datetime(combined["measured_at"], utc=True)
    combined = combined[combined["measured_at"] >= cutoff]
    logger.info("Loaded %d raw records from landing (last %dh)", len(combined), lookback_hours)
    return combined

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    initial_len = len(df)
    df = df.dropna(subset=["location_id", "parameter", "value", "measured_at"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    valid_mask = pd.Series([True] * len(df), index=df.index)
    for param, (lo, hi) in VALID_RANGES.items():
        param_mask = df["parameter"] == param
        out_of_range = param_mask & ((df["value"] < lo) | (df["value"] > hi))
        valid_mask = valid_mask & ~out_of_range
    df = df[valid_mask]
    df = df.drop_duplicates(subset=["location_id", "parameter", "measured_at"])
    logger.info("Cleaned data: %d → %d records (dropped %d)", initial_len, len(df), initial_len - len(df))
    return df.reset_index(drop=True)

def compute_hourly_averages(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["hour_bucket"] = df["measured_at"].dt.floor("h")
    group_cols = ["location_id", "location_name", "city", "country", "latitude", "longitude", "parameter", "hour_bucket"]
    agg = (
        df.groupby(group_cols, dropna=False)["value"]
        .agg(avg_value="mean", min_value="min", max_value="max", reading_count="count")
        .reset_index()
    )
    agg["avg_value"] = agg["avg_value"].round(4)
    agg["min_value"] = agg["min_value"].round(4)
    agg["max_value"] = agg["max_value"].round(4)
    agg["computed_at"] = datetime.now(timezone.utc)
    logger.info("Computed %d hourly average rows", len(agg))
    return agg

def compute_daily_averages(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["day_bucket"] = df["measured_at"].dt.date
    group_cols = ["location_id", "location_name", "city", "country", "latitude", "longitude", "parameter", "day_bucket"]
    def p95(x):
        return np.percentile(x, 95) if len(x) > 0 else np.nan
    agg = (
        df.groupby(group_cols, dropna=False)["value"]
        .agg(
            avg_value="mean",
            min_value="min",
            max_value="max",
            p95_value=p95,
            reading_count="count",
        )
        .reset_index()
    )
    agg["avg_value"] = agg["avg_value"].round(4)
    agg["min_value"] = agg["min_value"].round(4)
    agg["max_value"] = agg["max_value"].round(4)
    agg["p95_value"] = agg["p95_value"].round(4)
    agg["computed_at"] = datetime.now(timezone.utc)
    logger.info("Computed %d daily average rows", len(agg))
    return agg

def compute_latest_readings(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    latest = (
        df.sort_values("measured_at", ascending=False)
        .drop_duplicates(subset=["location_id", "parameter"])
        .copy()
    )
    def add_aqi(row):
        if row["parameter"] == "pm25":
            aqi_score, category = compute_aqi_pm25(row["value"])
            return category
        return None
    latest["aqi_category"] = latest.apply(add_aqi, axis=1)
    latest = latest[["location_id", "location_name", "city", "country", "latitude", "longitude",
                      "parameter", "value", "unit", "measured_at", "aqi_category"]]
    logger.info("Computed %d latest reading rows", len(latest))
    return latest

def compute_city_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["day_bucket"] = df["measured_at"].dt.date
    agg = (
        df.groupby(["city", "country", "parameter", "day_bucket"], dropna=False)["value"]
        .agg(avg_value="mean", max_value="max", station_count=lambda x: x.index.nunique())
        .reset_index()
    )
    def add_aqi_info(row):
        if row["parameter"] == "pm25" and pd.notna(row["avg_value"]):
            score, category = compute_aqi_pm25(row["avg_value"])
            return pd.Series({"aqi_score": score, "aqi_category": category})
        return pd.Series({"aqi_score": None, "aqi_category": None})
    aqi_cols = agg.apply(add_aqi_info, axis=1)
    agg = pd.concat([agg, aqi_cols], axis=1)
    agg["avg_value"] = agg["avg_value"].round(4)
    agg["max_value"] = agg["max_value"].round(4)
    agg["computed_at"] = datetime.now(timezone.utc)
    logger.info("Computed %d city daily summary rows", len(agg))
    return agg

def write_processed_parquet(df: pd.DataFrame, table_name: str, run_ts: Optional[datetime] = None) -> Optional[Path]:
    if df.empty:
        return None
    if run_ts is None:
        run_ts = datetime.now(timezone.utc)
    out_dir = PROCESSED_DIR / table_name / f"year={run_ts.year}" / f"month={run_ts.month:02d}" / f"day={run_ts.day:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{table_name}_{run_ts.strftime('%H%M%S')}.parquet"
    out_path = out_dir / filename
    df.to_parquet(out_path, index=False, engine="pyarrow", compression="snappy")
    logger.info("Wrote processed parquet %s (%d rows)", out_path, len(df))
    return out_path

def run_processing_pipeline(lookback_hours: int = 48, run_ts: Optional[datetime] = None) -> dict:
    if run_ts is None:
        run_ts = datetime.now(timezone.utc)
    raw_df = load_landing_parquet(lookback_hours)
    if raw_df.empty:
        logger.warning("No raw data found — skipping processing pipeline")
        return {"status": "no_data", "hourly_rows": 0, "daily_rows": 0, "latest_rows": 0}
    clean_df = clean_dataframe(raw_df)
    hourly_df = compute_hourly_averages(clean_df)
    daily_df = compute_daily_averages(clean_df)
    latest_df = compute_latest_readings(clean_df)
    city_summary_df = compute_city_daily_summary(clean_df)
    write_processed_parquet(hourly_df, "hourly_averages", run_ts)
    write_processed_parquet(daily_df, "daily_averages", run_ts)
    write_processed_parquet(latest_df, "latest_readings", run_ts)
    write_processed_parquet(city_summary_df, "city_daily_summary", run_ts)
    return {
        "status": "success",
        "raw_rows": len(raw_df),
        "clean_rows": len(clean_df),
        "hourly_rows": len(hourly_df),
        "daily_rows": len(daily_df),
        "latest_rows": len(latest_df),
        "city_summary_rows": len(city_summary_df),
    }