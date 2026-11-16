# ADR 001: Medallion Lakehouse Architecture Pattern

## Status
**Accepted**

## Context
E-commerce data platforms handle both operational transactions (orders, payments, fulfillment) and analytical requirements (retention cohorts, revenue forecasting, carrier SLA analysis). Direct analytical querying against the production OLTP database leads to query contention and resource exhaustion. Conversely, traditional nightly batch data warehouses lose fidelity and lack lineage when source schemas drift.

## Decision
We adopt the **Medallion Architecture** pattern consisting of three distinct layers:
1. **Bronze Layer (Raw):** Preserves immutable snapshots of operational source data directly in Parquet/Delta format, enriched with operational metadata audit columns (`_ingested_at`, `_run_id`, `_source_table`, `_record_hash`).
2. **Silver Layer (Cleaned & Conformed):** Applies standardized cleaning, trims whitespace, standardizes casing, deduplicates records using window functions (`ROW_NUMBER()`), and quarantines corrupt records without crashing downstream pipelines. Implements Slowly Changing Dimensions (SCD Type 2).
3. **Gold Layer (Curated & Aggregated):** Structures data into an optimized Kimball Star Schema (surrogate keys, conformed dimensions, transaction facts) and pre-computes 6 high-value business data marts.

## Consequences
### Positive:
- **Reproducibility:** Raw data is preserved immutably; any pipeline logic change can be replayed from Bronze.
- **Performance:** Complex joins and aggregations are pre-computed in Gold and business marts, enabling sub-second query response in Streamlit.
- **Auditability:** Every row in Gold can be traced back to its specific ingestion batch, source table, and record hash.

### Negative / Trade-offs:
- Increased storage footprint due to storing data across Bronze, Silver, and Gold.
- Orchestration requires careful dependency management between layers.
