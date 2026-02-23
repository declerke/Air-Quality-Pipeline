# 🌍 AeroPulse: Real-Time Air Quality Engineering & Observability

**AeroPulse** is an automated, cloud-native data pipeline designed to monitor and analyze global air quality data with a focus on East African urban centers. This project demonstrates a production-ready **ETL/ELT** architecture, moving from raw API ingestion to multi-tiered Parquet storage, SQL-driven analytics, and real-time observability.

---

## 🎯 The Problem
Urban air pollution is a critical health metric, yet data is often fragmented or inaccessible. This project solves three key engineering challenges:
1.  **Ingestion at Scale:** Handling high-velocity sensor data from the OpenAQ v3 API across multiple countries.
2.  **Data Reliability:** Implementing a multi-stage quality gate (Data Quality Logs) to prevent "garbage-in, garbage-out" scenarios in public health reporting.
3.  **Actionable Observability:** Providing sub-minute refreshes for PM2.5 monitoring and automated threshold breach detection.

---

## 🏗️ Architecture



```text
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  OpenAQ API v3  │      │  Apache Airflow │      │   PostgreSQL    │
│ (Global Sensors)│ ───▶│ (Orchestration) │ ───▶ │ (Analytic Hub)  │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                 │                       │
                         ┌─────────────────┐      ┌─────────────────┐
                         │ Partitioned Lake│      │    Grafana      │
                         │ (Local Parquet) │      │  (Observability)│
                         └─────────────────┘      └─────────────────┘
```

---

## 🚀 Key Features

* **Tiered Data Lake:** Implements a Medallion-style architecture using **Partitioned Parquet** (`country/year/month/day`) for cost-effective, high-speed storage.
* **Quality Guardrails:** A dedicated `dag_03_quality_checks` ensures data freshness, schema integrity, and value range validation before analytics are served.
* **Infrastructure as Code:** Fully containerized environment using **Docker Compose**, with pre-provisioned Grafana dashboards and Postgres schemas.
* **Smart Analytics:** SQL-driven categorization of Air Quality Index (AQI) levels and automated detection of WHO threshold breaches.

---

## 🛠️ Technical Stack
| Layer | Tools | Key Skill |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow | DAG scheduling, task dependencies, and error handling. |
| **Data Processing** | Python, Pandas, PyArrow | API integration and Parquet partitioning logic. |
| **Database** | PostgreSQL | Schema design, Materialized Views, and SQL optimization. |
| **Storage** | Parquet | Columnar storage for big data performance. |
| **Observability** | Grafana | Time-series visualization and dynamic templating. |
| **DevOps** | Docker, Docker Compose | Container orchestration and environmental consistency. |

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
2.  Enable `dag_01_ingest_raw`.
3.  Once raw data appears in `data/landing`, enable `dag_02_process_load` and `dag_03_quality_checks`.

---

## 📈 Monitoring & Quality
The system monitors itself through a **Data Quality Log** table:
* **Freshness Check:** Alerts if the time since the last reading exceeds 3 hours.
* **Completeness:** Logs the number of records failed during transformation.
* **Accuracy:** Validates that PM2.5 values fall within realistic atmospheric ranges (0-500 µg/m³).

---

## 🎓 Skills Demonstrated
* **ETL/ELT Best Practices:** Managing state between API, file system, and database.
* **Big Data Storage:** Implementing partition pruning with Parquet for efficient querying.
* **Analytic SQL:** Crafting complex views to derive AQI categories and threshold breaches.
* **Full-Stack Data Engineering:** Managing the entire lifecycle from raw data capture to executive-level visualization.
