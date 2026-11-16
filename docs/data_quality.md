# 🛡️ OrderFlow Data Quality Framework & Assertions

OrderFlow treats data quality as code. Automated verification gates guard against data corruption, reconciliation errors, and broken foreign key references.

---

## 1. Quality Check Taxonomy & Severities

| Severity | Definition | Pipeline Action on Failure |
| :--- | :--- | :--- |
| **CRITICAL** | Violations that corrupt financial reporting or break referential integrity (e.g. Unreconciled revenue, duplicate PKs). | **Hard Stop:** Aborts pipeline execution, blocks database publishing, alerts on-call. |
| **WARNING** | Data anomalies that do not immediately halt operations (e.g. slight delivery delay anomalies, missing non-critical dimensions). | **Soft Warning:** Logs to audit table, sends warning notification, allows pipeline to proceed. |

---

## 2. Core Quality Checks & Formulas

### 2.1 Financial Reconciliation Assertions
- **Target Table:** `fact_orders`
- **Severity:** `CRITICAL`
- **Formula:**
  $$\text{Order Total} = \text{Subtotal} - \text{Discount Amount} + \text{Tax Amount} + \text{Shipping Amount}$$
- **Assertion:**
  $$\left| \text{order\_total} - (\text{subtotal} - \text{discount\_amount} + \text{tax\_amount} + \text{shipping\_amount}) \right| \le \$0.02$$
- **Tolerance:** $\$0.02$ allowed strictly for standard floating-point / rounding boundary differences across multi-currency operations.

### 2.2 Primary Key Uniqueness
- **Target Tables:** `dim_customer.customer_id`, `dim_product.product_id`, `fact_orders.order_id`, `fact_order_items.order_item_id`.
- **Severity:** `CRITICAL`
- **Assertion:**
  $$\text{COUNT}(*) = \text{COUNT}(\text{DISTINCT } \text{primary\_key})$$

### 2.3 Mandatory Not-Null Checks
- **Target Fields:**
  - `orders.order_id`, `orders.customer_id`, `orders.order_total`, `orders.purchase_timestamp`
  - `customers.customer_id`, `customers.email`
  - `products.product_id`, `products.unit_price`, `products.cost_price`
- **Severity:** `CRITICAL`

### 2.4 Referential Integrity (Orphan Detection)
- **Target Tables:**
  - `fact_orders.customer_id` $\subset$ `dim_customer.customer_id`
  - `fact_order_items.order_id` $\subset$ `fact_orders.order_id`
  - `fact_order_items.product_id` $\subset$ `dim_product.product_id`
  - `fact_payments.order_id` $\subset$ `fact_orders.order_id`
- **Severity:** `CRITICAL`
- **Failure Threshold:** 0 orphan records allowed.

### 2.5 Categorical Value Validity
- **Target Fields:**
  - `orders.order_status` $\in$ `['delivered', 'shipped', 'in_transit', 'processing', 'cancelled', 'returned']`
  - `payments.payment_method` $\in$ `['credit_card', 'boleto', 'pix', 'voucher', 'debit_card']`
  - `payments.payment_status` $\in$ `['captured', 'pending', 'failed', 'refunded']`

---

## 3. Data Quality Audit Log Table (`audit_data_quality`)

Every check executed during lakehouse processing writes an audit record to `analytics.audit_data_quality`:

```sql
CREATE TABLE audit_data_quality (
    check_id VARCHAR(100) PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    check_name VARCHAR(100) NOT NULL,
    check_status VARCHAR(20) NOT NULL,  -- 'PASSED' or 'FAILED'
    records_checked BIGINT NOT NULL,
    records_passed BIGINT NOT NULL,
    records_failed BIGINT NOT NULL,
    failure_percentage NUMERIC(5, 2) NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,
    error_details JSONB
);
```

This table is directly monitored by Page 07 (`Pipeline Monitor & Data Quality`) in the Streamlit application.
