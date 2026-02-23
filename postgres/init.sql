CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS processed;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.measurements (
    id BIGSERIAL PRIMARY KEY,
    location_id BIGINT,
    location_name TEXT,
    city TEXT,
    country TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    parameter TEXT,
    value DOUBLE PRECISION,
    unit TEXT,
    measured_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    source_file TEXT,
    UNIQUE (location_id, parameter, measured_at)
);

CREATE INDEX IF NOT EXISTS idx_raw_measurements_country ON raw.measurements(country);
CREATE INDEX IF NOT EXISTS idx_raw_measurements_parameter ON raw.measurements(parameter);
CREATE INDEX IF NOT EXISTS idx_raw_measurements_measured_at ON raw.measurements(measured_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_measurements_city ON raw.measurements(city);

CREATE TABLE IF NOT EXISTS processed.hourly_averages (
    id BIGSERIAL PRIMARY KEY,
    location_id BIGINT,
    location_name TEXT,
    city TEXT,
    country TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    parameter TEXT,
    hour_bucket TIMESTAMPTZ,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    reading_count INTEGER,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (location_id, parameter, hour_bucket)
);

CREATE INDEX IF NOT EXISTS idx_hourly_city ON processed.hourly_averages(city);
CREATE INDEX IF NOT EXISTS idx_hourly_parameter ON processed.hourly_averages(parameter);
CREATE INDEX IF NOT EXISTS idx_hourly_bucket ON processed.hourly_averages(hour_bucket DESC);
CREATE INDEX IF NOT EXISTS idx_hourly_country ON processed.hourly_averages(country);

CREATE TABLE IF NOT EXISTS processed.daily_averages (
    id BIGSERIAL PRIMARY KEY,
    location_id BIGINT,
    location_name TEXT,
    city TEXT,
    country TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    parameter TEXT,
    day_bucket DATE,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    p95_value DOUBLE PRECISION,
    reading_count INTEGER,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (location_id, parameter, day_bucket)
);

CREATE INDEX IF NOT EXISTS idx_daily_city ON processed.daily_averages(city);
CREATE INDEX IF NOT EXISTS idx_daily_parameter ON processed.daily_averages(parameter);
CREATE INDEX IF NOT EXISTS idx_daily_bucket ON processed.daily_averages(day_bucket DESC);

CREATE TABLE IF NOT EXISTS processed.data_quality_log (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT,
    dag_id TEXT,
    check_name TEXT,
    status TEXT,
    records_checked INTEGER,
    records_failed INTEGER,
    details JSONB,
    checked_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.latest_readings (
    location_id BIGINT,
    location_name TEXT,
    city TEXT,
    country TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    parameter TEXT,
    value DOUBLE PRECISION,
    unit TEXT,
    measured_at TIMESTAMPTZ,
    aqi_category TEXT,
    PRIMARY KEY (location_id, parameter)
);

CREATE TABLE IF NOT EXISTS analytics.city_daily_summary (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    country TEXT,
    parameter TEXT,
    day_bucket DATE,
    avg_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    station_count INTEGER,
    aqi_score DOUBLE PRECISION,
    aqi_category TEXT,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (city, parameter, day_bucket)
);

CREATE INDEX IF NOT EXISTS idx_city_summary_city ON analytics.city_daily_summary(city);
CREATE INDEX IF NOT EXISTS idx_city_summary_parameter ON analytics.city_daily_summary(parameter);
CREATE INDEX IF NOT EXISTS idx_city_summary_day ON analytics.city_daily_summary(day_bucket DESC);

CREATE OR REPLACE VIEW analytics.nairobi_pm25_trend AS
SELECT
    hour_bucket,
    city,
    avg_value,
    max_value,
    reading_count
FROM processed.hourly_averages
WHERE city ILIKE '%nairobi%'
  AND parameter = 'pm25'
ORDER BY hour_bucket DESC;

CREATE OR REPLACE VIEW analytics.global_city_comparison AS
SELECT
    city,
    country,
    parameter,
    day_bucket,
    avg_value,
    aqi_category
FROM analytics.city_daily_summary
WHERE parameter IN ('pm25', 'pm10', 'no2', 'o3')
ORDER BY day_bucket DESC, city;

CREATE OR REPLACE VIEW analytics.threshold_breaches AS
SELECT
    l.location_name,
    l.city,
    l.country,
    l.parameter,
    l.value,
    l.measured_at,
    CASE
        WHEN l.parameter = 'pm25' AND l.value > 35.4 THEN 'PM2.5 exceeds WHO 24h guideline'
        WHEN l.parameter = 'pm10' AND l.value > 154  THEN 'PM10 exceeds WHO 24h guideline'
        WHEN l.parameter = 'no2'  AND l.value > 200  THEN 'NO2 exceeds WHO 1h guideline'
        WHEN l.parameter = 'o3'   AND l.value > 100  THEN 'O3 exceeds WHO 8h guideline'
    END AS breach_reason
FROM analytics.latest_readings l
WHERE
    (l.parameter = 'pm25' AND l.value > 35.4) OR
    (l.parameter = 'pm10' AND l.value > 154)  OR
    (l.parameter = 'no2'  AND l.value > 200)  OR
    (l.parameter = 'o3'   AND l.value > 100);