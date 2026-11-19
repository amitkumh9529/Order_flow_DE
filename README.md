# OrderFlow: Enterprise E-Commerce Cloud Data Platform

[![CI Pipeline](https://github.com/orderflow/orderflow-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/orderflow/orderflow-data-platform)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Architecture](https://img.shields.io/badge/architecture-Medallion%20Lakehouse-orange.svg)](#architecture)
[![Delta Lake](https://img.shields.io/badge/data--format-Delta%20Lake%20%7C%20Parquet-blue.svg)](https://delta.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

OrderFlow is a production-grade cloud enterprise e-commerce data engineering platform. It ingests, validates, stores, and transforms high-volume operational e-commerce data into a high-performance **Medallion Lakehouse** (Bronze, Silver, Gold Delta tables) using **Supabase PostgreSQL**, **AWS S3**, **Databricks (PySpark & Delta Lake)**, **Apache Airflow**, and **Streamlit** business intelligence analytics.

---

## 1. High-Level Architecture

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        1. SOURCE (OLTP)                                │
│                     Supabase PostgreSQL                               │
│                   ('app' schema: 10 tables)                            │
│     customers, catalog, sellers, warehouses, orders, line items,       │
│           payments, shipments, inventory, web clickstream              │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Incremental Python Extractor (run_ingestion.py)
                                    │ Sliding watermarked batch windows (ops.watermarks)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                     2. BRONZE LAYER (Landing)                          │
│                            AWS S3                                      │
│        s3://order-flow-de/raw/{entity}/year=.../month=.../             │
│        - Columnar Apache Parquet + Snappy compression                  │
│        - Audit metadata: _extracted_at, _batch_id, _record_checksum    │
│        - Cryptographic SHA-256 batch manifest verification             │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Databricks Auto Loader / read_parquet
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│          3. TRANSFORMATION ENGINE & LAKEHOUSE (Delta Lake)             │
│                       Databricks & PySpark                             │
│                                                                        │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │  Silver Layer (Delta Tables):                                  │   │
│   │  - Cleansing, deduplication, schema enforcement                │   │
│   │  - Quarantine pattern for malformed records                    │   │
│   │  - SCD Type 2 tracking on dim_customer_history                 │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
│                                   │ PySpark Dimensional Joins & Aggs   │
│                                   ▼                                    │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │  Gold Layer (Delta Tables):                                    │   │
│   │  - Star Schema: 8 Dimensions & 6 Fact tables                   │   │
│   │  - 6 Pre-computed Business Data Marts (sales, cohorts, etc.)   │   │
│   │  - Data Quality & Financial Reconciliation Gates               │   │
│   │    (Order Total = Subtotal - Discount + Tax + Shipping)        │   │
│   └────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Gold Exporter / Publisher (publish_gold.py)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        4. CONSUMPTION & BI                             │
│     - Supabase / PostgreSQL Serving Layer ('analytics' schema)         │
│     - Streamlit BI Dashboard (dashboard/app.py - 7 analytics pages)    │
│     - Direct Databricks SQL Queries on Gold Delta Lake                 │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Technology Stack

| Layer | Component | Description |
|---|---|---|
| **Language** | Python 3.11+ | Ingestion engine, synthetic generators, testing, and validation |
| **Operational Source (OLTP)** | Supabase PostgreSQL | Source transactional database (`app` schema) and metadata tracking (`ops` schema) |
| **Bronze Landing** | AWS S3 | Immutable columnar raw Parquet files with SHA-256 batch manifests |
| **Lakehouse Engine** | Databricks & PySpark | Distributed transformations across Bronze, Silver, and Gold Delta tables |
| **Data Format** | Delta Lake & Apache Parquet | ACID transactions, schema enforcement, time-travel, and columnar compaction |
| **Orchestration** | Apache Airflow | Scheduled DAGs for ingestion, Databricks handoff, and Gold publishing |
| **Data Quality & Audit** | Pytest & Custom Audits | Schema validation, referential integrity, and automated financial reconciliation |
| **Analytics & UI** | Streamlit & Plotly | Interactive 8-page business intelligence and executive KPI dashboard |

---

## 3. Repository Structure

```text
Order_Flow/
├── config/                 # Pydantic Settings, environment and logging configs
├── sql/
│   ├── migrations/         # 12 PostgreSQL operational table migrations (app & ops)
│   ├── analytics/          # Dimensional Star Schema and Data Mart DDL (analytics)
│   └── quality/            # SQL-based source verification scripts
├── scripts/                # CLI runners: seed, ingest, handoff, publish, demo
│   ├── seed_database.py             # Operational DB seeder (small & standard profiles)
│   ├── run_ingestion.py             # Incremental/Full batch extractor to Parquet
│   ├── prepare_databricks_handoff.py# Packages landing files for Databricks Lakehouse
│   ├── publish_gold.py              # Loads and upserts Gold models into analytics DB
│   └── generate_demo_report.py      # End-to-end single-command pipeline verification
├── src/orderflow/          # Core Python framework
│   ├── database/           # Connection pooling, query builders, and readers/writers
│   ├── ingestion/          # Watermarked batch incremental and full extractors
│   ├── storage/            # S3 client, Parquet writer, and SHA-256 manifests
│   ├── generator/          # Realistic synthetic e-commerce data generator
│   ├── quality/            # Multi-layer data quality and financial reconciliation
│   ├── publishing/         # Gold loader, local lakehouse runner, and upsert engine
│   └── utils/              # Hashing, timestamps, and retry decorators
├── databricks/             # 15 Medallion notebooks (Bronze -> Silver -> Gold)
│   ├── 00_environment_setup.py      # Creates catalogs, schemas, and volumes
│   ├── 01_bronze_ingestion.py       # Raw Parquet landing to Bronze Delta tables
│   ├── 02-08_silver_*.py            # Cleansed & standardized Silver Delta tables
│   ├── 09_scd2_customer.py          # SCD Type 2 customer history tracking
│   ├── 10-11_gold_*.py              # Star Schema dimensional models and facts
│   ├── 12_gold_marts.py             # 6 analytical business marts
│   ├── 13_data_quality.py           # Automated financial reconciliation audits
│   └── 14_export_gold.py            # Exports Gold models for downstream serving
├── airflow/dags/           # Airflow DAGs (Ingestion, Lakehouse Handoff, Publish)
├── dashboard/              # 8-page Streamlit analytics dashboard
├── tests/                  # Unit, integration, quality, and idempotency tests
└── docs/                   # Complete architecture, schema, and deployment runbooks
```

---

## 4. Environment Configuration (`.env`)

Copy the example configuration file:
```bash
cp .env.example .env
```

Configure your environment credentials:

```ini
APP_ENV=local

# Operational & Analytics Database (Supabase PostgreSQL)
# Note: Use port 6543 (transaction pooler) with sslmode=require for cloud Supabase
SUPABASE_DB_URL=postgresql://postgres.[PROJECT_REF]:[PASSWORD]@aws-1-[REGION].pooler.supabase.com:6543/postgres?sslmode=require
SUPABASE_DB_READ_URL=postgresql://postgres.[PROJECT_REF]:[PASSWORD]@aws-1-[REGION].pooler.supabase.com:6543/postgres?sslmode=require
SUPABASE_DB_WRITE_URL=postgresql://postgres.[PROJECT_REF]:[PASSWORD]@aws-1-[REGION].pooler.supabase.com:6543/postgres?sslmode=require

# (Optional) For offline/local development without cloud dependencies, you can use SQLite:
# SUPABASE_DB_URL=sqlite:///orderflow.db

# AWS S3 (Bronze Landing)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_S3_BUCKET=order-flow-de

# Databricks Lakehouse
DATABRICKS_MODE=direct_s3  # Use 'direct_s3' for cloud S3, or 'free_handoff' for Community Edition
DATABRICKS_CATALOG=orderflow
DATABRICKS_SERVER_HOSTNAME=https://dbc-xxxx.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
DATABRICKS_TOKEN=dapi...

# Dashboard
STREAMLIT_PORT=8501
```

---

## 5. Quick Start Guide

### 1. Installation & Environment Setup

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# Linux / macOS:
source .venv/bin/activate

# Install dependencies in editable mode
pip install -e ".[dev,spark]"
# or with uv:
uv pip install -e .
```

---

### 2. Run the Full Pipeline in One Command (Demo / Verification)

To execute all 5 lifecycle stages (Data Generation $\rightarrow$ Ingestion & Parquet Landing $\rightarrow$ Medallion Transformation $\rightarrow$ Financial Reconciliation $\rightarrow$ Analytics Publishing) and generate an audit report in ~5 seconds:

```bash
python -m scripts.generate_demo_report
```

---

### 3. Step-by-Step Lifecycle Execution

#### Step A: Seed the Operational Database (OLTP)
Populates customers, products, sellers, warehouses, orders, line items, payments, shipments, inventory, and web clickstream events:
```bash
python -m scripts.seed_database --profile small --seed 42 --reset
```
*(Use `--profile standard` for large scale: 10,000 customers, 50,000 orders, 250,000 web events).*

#### Step B: Run Incremental Ingestion to S3 / Parquet Landing
Extracts watermarked batches from `app.*`, applies quality gates, stamps audit metadata, and writes partitioned Parquet files:
```bash
python -m scripts.run_ingestion --mode full
```

#### Step C: Prepare Databricks Lakehouse Handoff
Packages landing partitions and creates handoff manifests:
```bash
python -m scripts.prepare_databricks_handoff
```

#### Step D: Run Medallion Transformations in Databricks
Import the `databricks/` folder into your Databricks workspace and run the sequential notebooks:
1. `00_environment_setup.py`: Creates Unity Catalog schemas (`landing`, `bronze`, `silver`, `gold`, `quarantine`, `audit`).
2. `01_bronze_ingestion.py`: Ingests Parquet files into append-only Bronze Delta tables.
3. `02` through `09`: Cleanses, deduplicates, quarantines invalid records, and implements SCD Type 2 on customer history.
4. `10` through `12`: Constructs Gold Star Schema dimensions/facts and 6 business data marts.
5. `13_data_quality.py`: Evaluates financial reconciliation:
   $$\text{Order Total} = \text{Subtotal} - \text{Discount} + \text{Tax} + \text{Shipping}$$
6. `14_export_gold.py`: Emits optimized Gold models and verification manifests.

#### Step E: Publish to Analytics Serving Layer
```bash
python -m scripts.publish_gold
```

#### Step F: Launch the Interactive BI Dashboard
```bash
streamlit run dashboard/app.py
```
Open **`http://localhost:8501`** to explore:
- 📊 **Executive Overview**: High-level KPIs (revenue, margin, orders).
- 📈 **Sales Analytics**: Daily revenue trends, channel breakdown, and geography.
- 👥 **Customer Cohorts**: Retention matrices and customer LTV progression.
- 📦 **Product Performance**: Top sellers, return rates, and category velocity.
- 💳 **Payments & Audit**: Reconciliation variance, payment methods, and failure logs.
- 🚚 **Logistics & Deliveries**: Transit duration, carrier efficiency, and SLA compliance.
- 🏭 **Inventory Health**: Stockout alerts and replenishment points.
- 🔍 **Pipeline Observability**: Ingestion logs, batch freshness, and quality gate pass rates.

---

## 6. Running Tests

Run the Pytest suite covering unit tests, schema validation, data quality gates, and pipeline idempotency:

```bash
pytest
```

---

## 7. License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
