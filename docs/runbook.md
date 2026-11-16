# 🛠️ OrderFlow Operations Runbook

This runbook outlines operational procedures, troubleshooting guides, backfilling steps, and disaster recovery playbooks for platform operators.

---

## 1. Routine Operational Commands

### 1.1 Ingestion Health Check
Check current high-watermark state across all tables:
```bash
python -c "
from orderflow.database.connection import db_manager
from sqlalchemy import text
with db_manager.connect() as conn:
    rows = conn.execute(text('SELECT pipeline_name, source_table, last_successful_watermark, updated_at FROM ops.pipeline_watermarks')).fetchall()
    for r in rows:
        print(f'{r[0]:<25} | {r[1]:<15} | {r[2]} | {r[3]}')
"
```

### 1.2 Trigger Manual Extraction
Run full or incremental ingestion:
```bash
# Incremental extraction for active transaction tables
python scripts/run_ingestion.py --mode incremental

# Full refresh for reference dimensions (warehouses, sellers)
python scripts/run_ingestion.py --mode full --tables warehouses,sellers
```

### 1.3 Validate Gold Export Bundle
Verify data quality checks and file checksums before publishing:
```bash
python scripts/validate_gold_export.py --gold-dir data/gold_export
```

---

## 2. Incident Response Playbooks

### Incident 1: Ingestion Failure / Mid-Batch Database Drop
- **Symptom:** Airflow task `extract_orders` fails with `DatabaseConnectionError`.
- **Impact:** Incomplete Parquet files in staging, watermark not advanced.
- **Recovery Procedure:**
  1. Inspect logs to identify connection failure cause.
  2. The pipeline's automated watermark rollback ensures no records are skipped.
  3. Simply re-trigger the failed task in Airflow or rerun CLI:
     ```bash
     python scripts/run_ingestion.py --mode incremental --tables orders
     ```
  4. The 5-minute lookback window automatically re-extracts any boundary records. Deduplication at the Silver layer guarantees zero duplicate orders.

### Incident 2: Financial Reconciliation Check Failure
- **Symptom:** Quality gate fails with `FINANCIAL_RECONCILIATION_ORDER_TOTAL FAILED`.
- **Impact:** Downstream publishing to `analytics.fact_orders` is blocked by `PublishValidator`.
- **Diagnostic Query:**
  ```sql
  SELECT 
      order_id, subtotal, discount_amount, tax_amount, shipping_amount, order_total,
      (subtotal - discount_amount + tax_amount + shipping_amount) AS computed_total,
      ABS(order_total - (subtotal - discount_amount + tax_amount + shipping_amount)) AS delta
  FROM silver_orders
  WHERE ABS(order_total - (subtotal - discount_amount + tax_amount + shipping_amount)) > 0.02;
  ```
- **Remediation:**
  1. Determine if a new promo code or tax calculation rule was introduced upstream without updating the medallion logic.
  2. If upstream operational bug, quarantine affected `order_id`s in `silver_orders_quarantine`.
  3. Re-run Gold transformation.

### Incident 3: Missing S3 Landing Files or Checksum Mismatch
- **Symptom:** `PublishValidator` raises `ValidationError: Export manifest file checksum verification failed`.
- **Root Cause:** A Parquet file was modified, truncated, or corrupted in transit.
- **Remediation:**
  1. Identify the tampered file reported in error logs.
  2. Check disk integrity or re-export from Databricks/DuckDB:
     ```bash
     python scripts/publish_gold.py --force-recompute --gold-dir data/gold_export
     ```

---

## 3. Historical Backfill Procedure

To backfill historical data for a specific date range:
```bash
# 1. Reset watermark for target table to backfill start date
python -c "
from orderflow.ingestion.watermark_manager import WatermarkManager
wm = WatermarkManager()
wm.update_watermark('orderflow_ingestion', 'orders', '2026-01-01T00:00:00+00:00', 'backfill-init')
"

# 2. Trigger incremental extraction
python scripts/run_ingestion.py --mode incremental --tables orders

# 3. Execute Lakehouse transformation
python -c "
from orderflow.publishing.local_lakehouse_runner import LocalLakehouseRunner
runner = LocalLakehouseRunner()
runner.run_all(run_id='backfill-run')
"

# 4. Publish to Analytics
python scripts/publish_gold.py
```
