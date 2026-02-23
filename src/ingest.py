import os
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
LANDING_DIR = Path(os.getenv("AQ_DATA_LANDING", "/opt/airflow/data/landing"))
REQUEST_TIMEOUT = 30

CITY_COORDS = {
    "Nairobi": {"lat": -1.2921, "lon": 36.8219, "country": "KE"},
    "Mombasa": {"lat": -4.0435, "lon": 39.6682, "country": "KE"},
    "Kisumu": {"lat": -0.0917, "lon": 34.7680, "country": "KE"},
    "Nakuru": {"lat": -0.3031, "lon": 36.0800, "country": "KE"},
    "Eldoret": {"lat": 0.5143, "lon": 35.2698, "country": "KE"},
    "London": {"lat": 51.5074, "lon": -0.1278, "country": "GB"},
    "New York": {"lat": 40.7128, "lon": -74.0060, "country": "US"},
    "Beijing": {"lat": 39.9042, "lon": 116.4074, "country": "CN"},
    "Delhi": {"lat": 28.6139, "lon": 77.2090, "country": "IN"},
    "Lagos": {"lat": 6.5244, "lon": 3.3792, "country": "NG"}
}

PARAMETER_MAP = {
    "pm10": "pm10",
    "pm2_5": "pm25",
    "nitrogen_dioxide": "no2",
    "ozone": "o3",
    "carbon_monoxide": "co"
}

PYARROW_SCHEMA = pa.schema([
    pa.field("location_id", pa.int64()),
    pa.field("location_name", pa.string()),
    pa.field("city", pa.string()),
    pa.field("country", pa.string()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("parameter", pa.string()),
    pa.field("value", pa.float64()),
    pa.field("unit", pa.string()),
    pa.field("measured_at", pa.timestamp("us", tz="UTC")),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
])

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    reraise=True,
)
def _get(params: dict) -> dict:
    response = requests.get(OPEN_METEO_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()

def fetch_locations_by_country(country_code: str) -> list[dict]:
    locations = []
    for city, coords in CITY_COORDS.items():
        if coords["country"] == country_code:
            locations.append({
                "id": hash(city) % 10**8,
                "name": f"{city}_Meteo",
                "city": city,
                "country": country_code,
                "latitude": coords["lat"],
                "longitude": coords["lon"]
            })
    return locations

def fetch_latest_measurements(location_ids: list[int]) -> list[dict]:
    all_results = []
    for city, coords in CITY_COORDS.items():
        city_id = hash(city) % 10**8
        if city_id in location_ids:
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "hourly": "pm10,pm2_5,nitrogen_dioxide,ozone,carbon_monoxide",
                "timezone": "auto",
                "past_days": 1
            }
            try:
                data = _get(params)
                data["city_name"] = city
                data["city_id"] = city_id
                data["country_code"] = coords["country"]
                all_results.append(data)
            except Exception as e:
                logger.error("Error fetching Meteo data for %s: %s", city, e)
    return all_results

def parse_measurements(raw_results: list[dict]) -> list[dict]:
    records = []
    ingested_at = datetime.now(timezone.utc)
    for result in raw_results:
        city_name = result.get("city_name")
        city_id = result.get("city_id")
        country_code = result.get("country_code", "unknown")
        hourly = result.get("hourly", {})
        times = hourly.get("time", [])
        
        for i, t_str in enumerate(times):
            measured_at = pd.to_datetime(t_str, utc=True)
            for api_param, schema_param in PARAMETER_MAP.items():
                values = hourly.get(api_param, [])
                if i < len(values) and values[i] is not None:
                    records.append({
                        "location_id": city_id,
                        "location_name": f"{city_name}_Meteo",
                        "city": city_name,
                        "country": country_code,
                        "latitude": result.get("latitude"),
                        "longitude": result.get("longitude"),
                        "parameter": schema_param,
                        "value": float(values[i]),
                        "unit": "µg/m³",
                        "measured_at": measured_at,
                        "ingested_at": ingested_at
                    })
    return records

def write_parquet(records: list[dict], run_ts: datetime, country_code: str, batch_label: str) -> Optional[Path]:
    if not records:
        return None
    df = pd.DataFrame(records)
    partition_dir = (
        LANDING_DIR 
        / f"country={country_code}" 
        / f"year={run_ts.year}" 
        / f"month={run_ts.month:02d}" 
        / f"day={run_ts.day:02d}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{batch_label}_{run_ts.strftime('%H%M%S')}.parquet"
    output_path = partition_dir / filename
    table = pa.Table.from_pandas(df, schema=PYARROW_SCHEMA, preserve_index=False)
    pq.write_table(table, output_path, compression="snappy")
    return output_path

def ingest_country(country_code: str, run_ts: Optional[datetime] = None) -> dict:
    if run_ts is None:
        run_ts = datetime.now(timezone.utc)
    locations = fetch_locations_by_country(country_code)
    if not locations:
        return {"country": country_code, "locations": 0, "records": 0, "file": None}
    
    raw_results = fetch_latest_measurements([loc["id"] for loc in locations])
    records = parse_measurements(raw_results)
    output_path = write_parquet(records, run_ts, country_code, "latest")
    return {
        "country": country_code,
        "locations": len(locations),
        "records": len(records),
        "file": str(output_path) if output_path else None,
    }

def ingest_all(run_ts: Optional[datetime] = None) -> list[dict]:
    if run_ts is None:
        run_ts = datetime.now(timezone.utc)
    countries = list(set(c["country"] for c in CITY_COORDS.values()))
    results = []
    for code in countries:
        results.append(ingest_country(code, run_ts))
    return results
