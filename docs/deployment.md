# 🚀 OrderFlow Deployment & Environment Setup Guide

This guide covers deployment instructions for local development, Docker Compose orchestration, Supabase cloud configuration, AWS S3 buckets, and Databricks Free Edition.

---

## 1. Quick Start (Local Development)

### 1.1 Prerequisites
- Python 3.11+ (or 3.12/3.14)
- Git
- Virtual environment tool (e.g. `venv`, `uv`, or `conda`)

### 1.2 Installation
```bash
# Clone and enter workspace
git clone <repo-url>
cd orderflow

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\Activate.ps1

# Install package in editable mode with development dependencies
pip install -e ".[dev]"
```

### 1.3 Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Key configuration parameters:
```ini
ENVIRONMENT=development
DATABASE_URL=sqlite:///orderflow.db
ANALYTICS_DATABASE_URL=sqlite:///orderflow_analytics.db
AWS_S3_BUCKET_LANDING=orderflow-landing
AWS_DEFAULT_REGION=us-east-1
DATA_LANDING_LOCAL_DIR=data/landing
GOLD_EXPORT_DIR=data/gold_export
```

### 1.4 End-to-End Pipeline Execution
Run the full platform demonstration:
```bash
python scripts/generate_demo_report.py
```

Launch the Streamlit Analytics Dashboard:
```bash
streamlit run dashboard/app.py
```

---

## 2. Docker Compose Deployment

Run the complete multi-service stack with a single command:
```bash
docker-compose up -d
```

### Services Started:
- **`postgres-oltp`**: PostgreSQL 16 operational database (`port 5432`).
- **`postgres-analytics`**: PostgreSQL 16 serving data warehouse (`port 5433`).
- **`airflow-webserver`**: Apache Airflow UI (`port 8080`).
- **`orderflow-dashboard`**: Streamlit Executive Dashboard (`port 8501`).

Access Points:
- Streamlit Dashboard: `http://localhost:8501`
- Airflow Web UI: `http://localhost:8080` (user: `airflow`, pass: `airflow`)

---

## 3. Supabase Cloud Configuration

To connect OrderFlow to a live Supabase PostgreSQL instance:
1. Create a project at [supabase.com](https://supabase.com).
2. Navigate to **Project Settings -> Database** and copy the Connection String (URI).
3. Update `.env`:
   ```ini
   DATABASE_URL=postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres?sslmode=require
   ANALYTICS_DATABASE_URL=postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres?sslmode=require
   ```
4. Run schema migrations:
   ```bash
   python scripts/run_migrations.py
   ```
5. Seed synthetic operational data:
   ```bash
   python scripts/seed_database.py --profile standard --seed 42
   ```

---

## 4. Databricks Free Edition Integration

Databricks Community/Free Edition does not permit mounting external S3 buckets directly. OrderFlow solves this via a robust stage-and-handoff pipeline:

### 4.1 Stage Datasets
Run the automated preparation script:
```bash
python scripts/prepare_databricks_handoff.py --run-id latest --output-dir dbfs_upload
```

### 4.2 Run Notebooks in Databricks
Import notebooks from the `databricks/` directory into your Databricks workspace:
1. `00_environment_setup.py`: Configures Spark session and catalogs.
2. `01_bronze_ingestion.py`: Reads raw Parquet files from Volume/DBFS into Bronze Delta tables.
3. `02_silver_cleaning.py`: Performs standardizations and deduplication.
4. `04_silver_scd2_customers.py`: Applies SCD Type 2 merges.
5. `05_gold_dimensions.py` & `06_gold_facts.py`: Constructs Star Schema.
6. `07_gold_marts.py`: Builds analytical business marts.
7. `14_export_gold.py`: Emits optimized Gold Parquet files and SHA-256 manifest for publishing.

### 4.3 Publish Gold to Analytics
Once Databricks exports the Gold bundle:
```bash
python scripts/publish_gold.py --gold-dir dbfs_upload/gold
```
