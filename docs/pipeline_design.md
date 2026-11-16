# ⚙️ OrderFlow Pipeline Design & Mechanics

This document provides technical architectural details regarding the OrderFlow extraction engine, watermark state handling, idempotency, and transformation mechanics.

---

## 1. Incremental Ingestion Engine

### 1.1 The Lookback Window Strategy
To guarantee zero data loss in the presence of out-of-order commits, clock skew, or concurrent transactions, OrderFlow utilizes an incremental extraction algorithm with an automated **5-minute lookback window**:

```text
Extraction Lower Bound = max(last_successful_watermark - 5 minutes, pipeline_epoch)
Extraction Upper Bound = current_batch_timestamp
```

```sql
SELECT *
FROM app.orders
WHERE updated_at >= :lower_bound
  AND updated_at <= :upper_bound
ORDER BY updated_at ASC, order_id ASC;
```

### 1.2 Deterministic Tie-Breaking
Ordering strictly by `updated_at ASC` can produce non-deterministic pagination if multiple transactions commit with the exact same microsecond timestamp. OrderFlow always appends a secondary ordering key on the primary key:
`ORDER BY updated_at ASC, <primary_key> ASC`

### 1.3 Atomic Watermark State Management
- Watermarks are queried from `ops.pipeline_watermarks` at the start of each extraction run.
- Watermark persistence occurs **only after**:
  1. All Parquet batch files are flushed to disk.
  2. SHA-256 file checksums are validated.
  3. The S3 multi-part upload (or local landing write) is verified via `head_object` content-length check.
- If any batch fails or crashes mid-flight, the watermark is **not advanced**, allowing safe retry on the subsequent run without data loss.

---

## 2. Partitioning Strategy & Metadata Architecture

### 2.1 File System Hierarchy
Raw landing files are partitioned by ingestion date and unique orchestration execution ID:
```text
s3://orderflow-landing/
└── raw/
    ├── customers/
    │   └── ingestion_date=2026-11-14/
    │       └── run_id=run-853f440b/
    │           ├── part-000.parquet
    │           └── part-001.parquet
    ├── orders/
    │   └── ingestion_date=2026-11-14/
    │       └── run_id=run-853f440b/
    │           ├── part-000.parquet
    │           └── part-001.parquet
    └── manifests/
        └── manifest_run-853f440b.json
```

### 2.2 Standard Operational Audit Columns
Every extracted record is enriched with 6 platform audit columns:
| Column Name | Type | Description |
| :--- | :--- | :--- |
| `_ingested_at` | `TIMESTAMP` | UTC timestamp when the ingestion batch processed the record. |
| `_run_id` | `VARCHAR(100)` | Orchestration run UUID. |
| `_source_system` | `VARCHAR(50)` | Source system tag (`supabase_oltp`). |
| `_source_table` | `VARCHAR(100)` | Source table name. |
| `_schema_version` | `VARCHAR(20)` | Schema contract version (e.g. `1.0.0`). |
| `_record_hash` | `CHAR(64)` | Deterministic SHA-256 hash of all non-audit business fields. |

---

## 3. Idempotency & Deduplication

Because of the 5-minute lookback window and potential task retries, raw Bronze files will naturally contain overlapping records.

### 3.1 Silver Layer Deduplication
The Silver layer applies deterministic deduplication before computing dimensional models:
```sql
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY updated_at DESC, _ingested_at DESC
        ) AS rn
    FROM bronze_orders
)
SELECT * EXCLUDE (rn)
FROM ranked 
WHERE rn = 1;
```

### 3.2 Slowly Changing Dimension (SCD) Type 2 Mechanics
For customer address and demographic changes, historical versions are preserved:
- `effective_from`: Timestamp when the customer state became active.
- `effective_to`: Timestamp when superseded by a newer record (or `NULL` if active).
- `is_current`: Boolean flag (`TRUE` for active version, `FALSE` for superseded historical versions).

```sql
WITH ordered AS (
    SELECT *,
        updated_at AS effective_from,
        LEAD(updated_at) OVER (
            PARTITION BY customer_id 
            ORDER BY updated_at ASC
        ) AS next_update
    FROM silver_customers
)
SELECT
    *,
    COALESCE(next_update, '9999-12-31 23:59:59') AS effective_to,
    CASE WHEN next_update IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM ordered;
```

---

## 4. Disaster Recovery & Failure Modes

1. **Extraction Failure:** Watermark rollback prevents skipped records.
2. **S3 Network Outage:** Exponential backoff retry with Tenacity (up to 3 attempts with jitter).
3. **Corrupt Batch / Schema Drift:** Quarantined to error tables without blocking healthy upstream records.
4. **Publish Failure:** Atomic database replace / staging table swap ensures the live analytics schema is never left in a partial or corrupt state.
