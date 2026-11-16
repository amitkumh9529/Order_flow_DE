# OrderFlow: Enterprise E-Commerce Data Platform

[![CI Pipeline](https://github.com/orderflow/orderflow-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/orderflow/orderflow-data-platform)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Architecture](https://img.shields.io/badge/architecture-Medallion%20Lakehouse-orange.svg)](#architecture)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

OrderFlow is a cloud-based enterprise e-commerce data engineering platform that ingests, processes, validates, stores, and transforms data across disparate operational systems into a high-performance Medallion Lakehouse (Bronze, Silver, Gold) with Apache Airflow orchestration and Streamlit business analytics.

---

## 1. Business Problem & Scenario

Fast-growing e-commerce businesses face fragmented data spread across independent operational services:
- **Disparate Systems**: Customer accounts, catalog metadata, multi-item orders, multiple payment attempts (UPI, Cards, COD), shipments, warehouse inventories, and web clickstream events live in silos.
- **Reporting Inconsistencies**: Late-arriving transactions and updates cause misaligned financial reconciliation.
- **Customer Identity Drift**: Customer contact and location details update without historical tracking (SCD Type 2).
- **Manual Data Dumps**: Downstream analytics rely on brittle, manual CSV exports rather than automated, validated, and idempotent ELT pipelines.

OrderFlow resolves these challenges through an end-to-end automated platform featuring incremental extraction, verifiable S3 cloud landing, Databricks Delta Lake medallion architecture, multi-layer data quality auditing, and interactive business intelligence.

---

## 2. High-Level Architecture

```text
                      OPERATIONAL SYSTEMS
                               |
              +----------------+----------------+
              |                |                |
           Orders          Customers         Products
          Payments         Inventory        Shipments
              |                |                |
              +----------------+----------------+
                               |
                               v
                     SUPABASE POSTGRESQL
                        (app schema)
                               |
                               v
                   INCREMENTAL PYTHON INGESTION
                               |
               +---------------+---------------+
               |                               |
               v                               v
          RAW EXTRACTION                 DATA QUALITY GATES
         (Watermarked Window)          (Schema, PK, Freshness)
               |
               v
            AWS S3
         (raw/ landing)
               |
               v
           DATABRICKS
               |
       +-------+-------+
       |               |
       v               v
     BRONZE        QUARANTINE
   (Raw Delta)   (Invalid Records)
       |
       v
     SILVER (Cleaned, Standardized, Deduplicated, SCD2 Customer History)
       |
       v
      GOLD (Dimensional Star Schema & 6 Business Data Marts)
       |
       v
   PUBLISHER & SUPABASE ANALYTICS (analytics schema)
       |
       v
  STREAMLIT DASHBOARDS & APIS
```

---

## 3. Technology Stack

| Layer | Component | Description |
|---|---|---|
| **Language** | Python 3.11+ | Core extraction, generators, testing, validation |
| **Operational DB** | Supabase PostgreSQL | Source OLTP database with `app`, `ops`, and `analytics` schemas |
| **Cloud Storage** | AWS S3 | Immutable raw Parquet files with SHA-256 manifests |
| **Lakehouse Engine** | Databricks & PySpark | Distributed transformations across Bronze, Silver, Gold Delta tables |
| **Data Format** | Delta Lake & Apache Parquet | ACID transactions, time travel, schema enforcement |
| **Orchestration** | Apache Airflow 3 | Scheduled DAGs for ingestion, handoff, and publishing |
| **Data Quality** | Custom Pytest & Spark Audits | Schema validation, referential integrity, financial reconciliation |
| **Analytics & UI** | Streamlit & Plotly | Interactive 8-page business intelligence dashboard |
| **CI/CD** | GitHub Actions | Automated linting (Ruff), typing, and unit/integration testing |

---

## 4. Repository Structure

```text
orderflow/
├── config/                 # Settings and pipeline configuration
├── sql/
│   ├── migrations/         # 12 PostgreSQL operational table migrations
│   ├── analytics/          # Dimensional models and data mart DDL
│   └── quality/            # SQL-based source verification scripts
├── scripts/                # CLI runners: seed, ingest, handoff, publish, report
├── src/orderflow/          # Core Python framework
│   ├── database/           # Connection pooling and source readers
│   ├── ingestion/          # Watermarked batch incremental extractors
│   ├── storage/            # S3 client and Parquet writer with manifests
│   ├── generator/          # Realistic synthetic e-commerce data generator
│   ├── quality/            # Multi-layer data quality and financial reconciliation
│   ├── publishing/         # Gold model loader and Supabase upsert
│   └── utils/              # Hashing, date utilities, retry decorators
├── databricks/             # 15 Medallion notebooks (Bronze -> Silver -> Gold)
├── airflow/dags/           # Airflow DAGs (Ingestion, Handoff, Publish)
├── dashboard/              # 8-page Streamlit analytics dashboard
├── tests/                  # Unit, integration, quality, and idempotency tests
└── docs/                   # Complete architecture, schema, and runbook documentation
```

---

## 5. Quick Start Guide

### 1. Environment Setup
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e ".[dev,spark]"
cp .env.example .env
```

### 2. Database Migrations & Seeding
```bash
# Seed synthetic data with small profile (1,500 customers, 5,000 orders)
python -m scripts.seed_database --profile small --seed 42
```

### 3. Run Ingestion Pipeline
```bash
# Extract incremental batches and upload to S3
python -m scripts.run_ingestion --mode full
```

### 4. Databricks Medallion Transformation
Run the sequential Databricks notebooks in `databricks/` or execute the local transformation pipeline:
```bash
python -m scripts.prepare_databricks_handoff
```

### 5. Publish to Analytics & Launch Dashboard
```bash
python -m scripts.publish_gold
streamlit run dashboard/app.py
```

---

## 6. Implementation Status
Refer to [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for full phase-by-phase completion details and test results.
