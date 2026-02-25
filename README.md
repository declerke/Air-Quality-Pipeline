# 🌍 AeroPulse: Predictive Air Quality Engineering & Observability

**AeroPulse** is an automated, cloud-native data pipeline designed to monitor and forecast global air quality data with a focus on East African urban centers. This project demonstrates a production-ready **ETL/ELT** architecture, moving from real-time **Open-Meteo API** ingestion to multi-tiered Parquet storage, SQL-driven analytics, and predictive observability.

---

## 🎯 The Problem
Urban air pollution is a critical health metric, but reactive data isn't enough for urban planning. This project solves three key engineering challenges:
1.  **Ingestion & Forecasting:** Leveraging the **Open-Meteo API** to ingest both historical sensor data and 5-day air quality forecasts.
2.  **Data Reliability:** Implementing a multi-stage quality gate (Data Quality Logs) to validate API responses before they hit the analytics layer.
3.  **Predictive Observability:** Providing real-time monitoring of current PM2.5 levels alongside forecast trends to detect upcoming "High Pollution" events.

---

## 🏗️ Architecture



```text
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Open-Meteo API  │     │   Apache Airflow│     │   PostgreSQL    │
│ (Real-time &    │────▶│ (Orchestration) │────▶│ (Analytic Hub)  │
│  Forecasts)     │     └─────────────────┘     └─────────────────┘
└─────────────────┘              │                       │
                        ┌─────────────────┐     ┌─────────────────┐
                        │ Partitioned Lake│     │    Grafana      │
                        │ (Local Parquet) │     │  (Observability)│
                        └─────────────────┘     └─────────────────┘
```

---

## 🚀 Key Features

* **Predictive Analytics:** Beyond historical data, the pipeline now processes **5-day forecasts** for PM2.5 and PM10, enabling proactive health alerts.
* **Tiered Data Lake:** Implements a Medallion-style architecture using **Partitioned Parquet** (`country/year/month/day`) for cost-effective, high-speed storage.
* **Quality Guardrails:** A dedicated `dag_03_quality_checks` ensures data freshness, schema integrity, and value range validation.
* **Infrastructure as Code:** Fully containerized environment using **Docker Compose**, with pre-provisioned Grafana dashboards and Postgres schemas.

---

## 🛠️ Technical Stack
| Layer | Tools | Key Skill |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow | DAG scheduling, task dependencies, and error handling. |
| **Data Source** | Open-Meteo API | High-resolution air quality and meteorological data. |
| **Data Processing** | Python, Pandas, PyArrow | API integration and Parquet partitioning logic. |
| **Database** | PostgreSQL | Schema design, Materialized Views, and SQL optimization. |
| **Storage** | Parquet | Columnar storage for big data performance. |
| **Observability** | Grafana | Time-series visualization and forecast-vs-actual tracking. |

---

## 📂 Project Structure
```text
air-quality-pipeline/
├── dags/                   # Airflow Orchestration
│   ├── dag_01_ingest_raw.py
│   ├── dag_02_process_load.py
│   └── dag_03_quality_checks.py
├── data/                   # Tiered Storage (Parquet)
│   ├── landing/            # Raw data (partitioned by country)
│   └── processed/          # Aggregated analytics
├── grafana/                # Visualization as Code
│   └── provisioning/       # Automated Dashboard/Datasource setup
├── postgres/               
│   └── init.sql            # Schema definitions (Raw, Processed, Analytics)
├── src/                    # Modular core logic (ingest.py, quality.py)
├── docker-compose.yml      # Orchestration of all services
└── requirements.txt        # Python dependency list
```

---

## ⚙️ Installation & Setup

### 1. Environment Setup
```bash
git clone [https://github.com/declerke/air-quality-pipeline.git](https://github.com/declerke/air-quality-pipeline.git)
cd air-quality-pipeline
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Launch the Ecosystem
```bash
docker-compose up -d
```
* **Airflow:** `http://localhost:8080` (Default: admin/admin)
* **Grafana:** `http://localhost:3000` (Default: admin/admin)

### 3. Run the Pipeline
1.  Navigate to the Airflow UI.
2.  Enable `dag_01_ingest_raw` (This calls the **Open-Meteo** endpoints for the configured coordinates).
3.  Once raw data appears in `data/landing`, enable `dag_02_process_load` and `dag_03_quality_checks`.

---

## 📈 Monitoring & Quality
The system monitors itself through a **Data Quality Log** table:
* **Freshness Check:** Alerts if the time since the last API update exceeds 1 hour.
* **Forecast Accuracy:** Future-ready hooks to compare predicted PM2.5 values against actual readings recorded later.
* **Accuracy:** Validates that PM2.5 values fall within realistic atmospheric ranges (0-500 µg/m³).

---

## 🎓 Skills Demonstrated
* **API Modernization:** Transitioning from static sensor sources to dynamic meteorological models (**Open-Meteo**).
* **Big Data Storage:** Implementing partition pruning with Parquet for efficient querying.
* **Analytic SQL:** Crafting complex views to derive AQI categories and threshold breaches.
* **Full-Stack Data Engineering:** Managing the entire lifecycle from raw data capture to executive-level visualization.
