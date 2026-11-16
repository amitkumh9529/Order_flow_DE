# ADR 002: Slowly Changing Dimensions (SCD Type 2) for Customer Entity

## Status
**Accepted**

## Context
Customers frequently update their contact details, shipping addresses, geographic locations, and customer loyalty tier segments over time. If customer attributes are simply updated in-place (SCD Type 1), historical orders placed prior to the move will be retroactively and incorrectly credited to the customer's new geographic state or segment, causing severe distortions in regional sales tax compliance and historical cohort analyses.

## Decision
We implement **Slowly Changing Dimensions Type 2 (SCD Type 2)** for the customer entity:
1. Every version of a customer profile is assigned a unique surrogate key (`customer_history_sk`).
2. Each historical version tracks:
   - `effective_from`: Timestamp when the specific profile state took effect.
   - `effective_to`: Timestamp when superseded by a newer version (`9999-12-31 23:59:59` for the current active version).
   - `is_current`: Boolean flag indicating whether the version is currently active.
3. In addition to `dim_customer_history` (SCD2), we publish `dim_customer` representing the latest snapshot (`is_current = TRUE`) to allow simple 1:1 joins for queries that do not require point-in-time historical reconstruction.

## Consequences
### Positive:
- **Historical Accuracy:** Orders placed in 2024 reflect the customer's address in 2024, preserving accurate sales tax and regional distribution reports.
- **Segment Migration Tracking:** Enables analytical queries on customer lifecycle movement (e.g. how many customers progressed from `Occasional` to `VIP`).

### Negative / Trade-offs:
- ETL logic requires window functions (`LEAD`) or complex MERGE statements.
- Fact tables must decide whether to link to natural key or surrogate historical key based on query intent.
