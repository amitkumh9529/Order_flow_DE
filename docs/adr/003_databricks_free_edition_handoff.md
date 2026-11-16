# ADR 003: Dual-Engine Lakehouse & Databricks Free Edition Stage-and-Handoff Pattern

## Status
**Accepted**

## Context
Databricks Community Edition (Free Edition) is an ideal, zero-cost cloud compute environment for learning, prototyping, and demonstration. However, Databricks Community Edition imposes strict cloud limitations:
1. Direct AWS S3 mounting via IAM credentials or instance profiles is disabled.
2. Inactivity terminates clusters after 2 hours.
3. Users and evaluators require a zero-dependency local execution mode that does not force them to create a paid Databricks account.

## Decision
We architect OrderFlow with a **Dual-Engine Architecture**:
1. **Cloud / Databricks Mode:**
   - Databricks notebooks (`databricks/00` to `databricks/14`) run full PySpark and Delta Lake workflows.
   - An automated staging script (`scripts/prepare_databricks_handoff.py`) exports raw landing Parquet files and generates upload bundles compatible with Databricks Volumes and DBFS.
   - Databricks exports the finalized Gold models and SHA-256 manifests to an export volume for publishing back to Supabase.
2. **Local Zero-Cloud Engine (`LocalLakehouseRunner`):**
   - Implemented using embedded DuckDB and PyArrow in Python.
   - Executes identical SQL transformations, SCD Type 2 logic, Star Schema builds, and Data Quality audit checks directly in-memory or on local disk in under 1 second.
   - Requires zero cloud accounts, zero paid clusters, and runs completely offline.

## Consequences
### Positive:
- **Portability & Cost:** Anyone can clone and run the full end-to-end lakehouse platform locally for $0.
- **Production Parity:** Production Databricks code and local DuckDB SQL share identical schema contracts, column names, formulas, and manifest formats.
- **Testability:** Unit and integration tests can execute complete lakehouse medallion cycles in GitHub Actions CI in 10 seconds.

### Negative / Trade-offs:
- Dual-engine maintenance requires ensuring both PySpark scripts and DuckDB runner adhere to identical business transformation rules.
