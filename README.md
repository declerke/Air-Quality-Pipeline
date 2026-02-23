# 🌍 AeroPulse: Real-Time Air Quality Engineering & Observability

**AeroPulse** is an automated, cloud-native data pipeline designed to monitor and analyze global air quality data with a focus on East African urban centers. [cite_start]This project demonstrates a production-ready **ETL/ELT** architecture, moving from raw API ingestion to multi-tiered Parquet storage, SQL-driven analytics, and real-time observability[cite: 1].

---

## 🎯 The Problem
Urban air pollution is a critical health metric, yet data is often fragmented or inaccessible. This project solves three key engineering challenges:
1.  [cite_start]**Ingestion at Scale:** Handling high-velocity sensor data from the OpenAQ v3 API across multiple countries[cite: 1].
2.  [cite_start]**Data Reliability:** Implementing a multi-stage quality gate (Data Quality Logs) to prevent "garbage-in, garbage-out" scenarios in public health reporting[cite: 1].
3.  [cite_start]**Actionable Observability:** Providing sub-minute refreshes for PM2.5 monitoring and automated threshold breach detection[cite: 1].

---

## 🏗️ Architecture



```text
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  OpenAQ API v3  │     │   Apache Airflow│     │   PostgreSQL    │
│ (Global Sensors)│────▶│ (Orchestration) │────▶│ (Analytic Hub)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                 │                       │
                        ┌─────────────────┐     ┌─────────────────┐
                        │ Partitioned Lake│     │    Grafana      │
                        │ (Local Parquet) │     │  (Observability)│
                        └─────────────────┘     └─────────────────┘
```

---

## 🚀 Key Features

* [cite_start]**Tiered Data Lake:** Implements a Medallion-style architecture using **Partitioned Parquet** (`country/year/month/day`) for cost-effective, high-speed storage[cite: 1].
* [cite_start]**Quality Guardrails:** A dedicated `dag_03_quality_checks` ensures data freshness, schema integrity, and value range validation before analytics are served[cite: 1].
* [cite_start]**Infrastructure as Code:** Fully containerized environment using **Docker Compose**, with pre-provisioned Grafana dashboards and Postgres schemas[cite: 1].
* [cite_start]**Smart Analytics:** SQL-driven categorization of Air Quality Index (AQI) levels and automated detection of WHO threshold breaches[cite: 1].

---

## 🛠️ Technical Stack
| Layer | Tools | Key Skill |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow | [cite_start]DAG scheduling, task dependencies, and error handling[cite: 1]. |
| **Data Processing** | Python, Pandas, PyArrow | [cite_start]API integration and Parquet partitioning logic[cite: 1]. |
| **Database** | PostgreSQL | [cite_start]Schema design, Materialized Views, and SQL optimization[cite: 1]. |
| **Storage** | Parquet | [cite_start]Columnar storage for big data performance[cite: 1]. |
| **Observability** | Grafana | [cite_start]Time-series visualization and dynamic templating[cite: 1]. |
| **DevOps** | Docker, Docker Compose | [cite_start]Container orchestration and environmental consistency[cite: 1]. |

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
[cite_start]``` [cite: 1]

---

## ⚙️ Installation & Setup

### 1. Environment Setup
```bash
git clone [https://github.com/YourUsername/air-quality-pipeline.git](https://github.com/YourUsername/air-quality-pipeline.git)
cd air-quality-pipeline
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Launch the Ecosystem
```bash
docker-compose up -d
```
* [cite_start]**Airflow:** `http://localhost:8080` (Default: admin/admin) [cite: 1]
* [cite_start]**Grafana:** `http://localhost:3000` (Default: admin/admin) [cite: 1]

### 3. Run the Pipeline
1.  [cite_start]Navigate to the Airflow UI[cite: 1].
2.  [cite_start]Enable `dag_01_ingest_raw`[cite: 1].
3.  [cite_start]Once raw data appears in `data/landing`, enable `dag_02_process_load` and `dag_03_quality_checks`[cite: 1].

---

## 📈 Monitoring & Quality
[cite_start]The system monitors itself through a **Data Quality Log** table[cite: 1]:
* [cite_start]**Freshness Check:** Alerts if the time since the last reading exceeds 3 hours[cite: 1].
* [cite_start]**Completeness:** Logs the number of records failed during transformation[cite: 1].
* [cite_start]**Accuracy:** Validates that PM2.5 values fall within realistic atmospheric ranges (0-500 µg/m³)[cite: 1].

---

## 🎓 Skills Demonstrated
* [cite_start]**ETL/ELT Best Practices:** Managing state between API, file system, and database[cite: 1].
* [cite_start]**Big Data Storage:** Implementing partition pruning with Parquet for efficient querying[cite: 1].
* [cite_start]**Analytic SQL:** Crafting complex views to derive AQI categories and threshold breaches[cite: 1].
* [cite_start]**Full-Stack Data Engineering:** Managing the entire lifecycle from raw data capture to executive-level visualization[cite: 1].
