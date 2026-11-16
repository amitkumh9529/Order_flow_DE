# 🗄️ OrderFlow Database Schema Specification

This document details the operational (`app`), operational logging (`ops`), and analytical (`analytics`) database schemas implemented across PostgreSQL/Supabase and local engines.

---

## 1. Operational OLTP Schema (`app`)

The operational database represents the production transactional system.

### 1.1 `app.customers`
- **Grain:** One row per customer.
- **Columns:**
  - `customer_id` (`UUID` / `TEXT`, PK): Unique customer identifier.
  - `first_name` (`VARCHAR(100)`): Customer first name.
  - `last_name` (`VARCHAR(100)`): Customer surname.
  - `email` (`VARCHAR(255)`, UNIQUE): Email address (sanitized format).
  - `phone` (`VARCHAR(50)`): Contact telephone number.
  - `street_address` (`VARCHAR(255)`): Billing/shipping street address.
  - `city` (`VARCHAR(100)`): City of residence.
  - `state` (`VARCHAR(100)`): State or province.
  - `postal_prefix` (`VARCHAR(20)`): Postal code prefix.
  - `customer_segment` (`VARCHAR(50)`): Segment (`VIP`, `Loyal`, `Standard`, `Occasional`).
  - `registration_date` (`DATE`): Registration date.
  - `created_at` (`TIMESTAMPTZ`): Creation timestamp.
  - `updated_at` (`TIMESTAMPTZ`): Last modified timestamp (Indexed for incremental extraction).

### 1.2 `app.products`
- **Grain:** One row per SKU.
- **Columns:**
  - `product_id` (`UUID`, PK): Unique SKU ID.
  - `product_name` (`VARCHAR(255)`): Catalog display name.
  - `category` (`VARCHAR(100)`): Top-level category.
  - `subcategory` (`VARCHAR(100)`): Secondary classification.
  - `brand` (`VARCHAR(100)`): Manufacturer/Brand.
  - `unit_price` (`NUMERIC(12, 2)`): Current retail sales price (> 0).
  - `cost_price` (`NUMERIC(12, 2)`): Unit acquisition cost (>= 0).
  - `weight_kg` (`NUMERIC(8, 3)`): Physical shipping weight in kilograms.
  - `is_active` (`BOOLEAN`): Active catalog flag.
  - `created_at`, `updated_at` (`TIMESTAMPTZ`).

### 1.3 `app.sellers`
- **Grain:** One row per marketplace seller.
- **Columns:**
  - `seller_id` (`UUID`, PK): Unique seller identifier.
  - `seller_name` (`VARCHAR(255)`): Merchant business name.
  - `city`, `state`, `postal_prefix` (`VARCHAR`).
  - `rating` (`NUMERIC(3, 2)`): Average seller rating (1.00 to 5.00).
  - `is_active` (`BOOLEAN`): Account active status.
  - `created_at`, `updated_at` (`TIMESTAMPTZ`).

### 1.4 `app.warehouses`
- **Grain:** One row per fulfillment distribution center.
- **Columns:**
  - `warehouse_id` (`UUID`, PK): Warehouse ID.
  - `warehouse_name` (`VARCHAR(100)`): Facility name.
  - `city`, `state`, `postal_prefix` (`VARCHAR`).
  - `capacity_sqft` (`INTEGER`): Total storage area.
  - `is_active` (`BOOLEAN`).
  - `created_at`, `updated_at` (`TIMESTAMPTZ`).

### 1.5 `app.orders`
- **Grain:** One row per placed order.
- **Columns:**
  - `order_id` (`UUID`, PK): Unique order reference.
  - `customer_id` (`UUID`, FK -> `app.customers`): Placing customer.
  - `order_status` (`VARCHAR(50)`): `delivered`, `shipped`, `in_transit`, `processing`, `cancelled`, `returned`.
  - `order_channel` (`VARCHAR(50)`): `web`, `mobile_app`, `marketplace`.
  - `order_currency` (`VARCHAR(10)`): ISO Currency code (`USD`, `EUR`, `BRL`).
  - `subtotal` (`NUMERIC(12, 2)`): Sum of product line totals.
  - `discount_amount` (`NUMERIC(12, 2)`): Applied promotion discounts.
  - `tax_amount` (`NUMERIC(12, 2)`): Calculated sales tax (18% standard).
  - `shipping_amount` (`NUMERIC(12, 2)`): Calculated freight delivery fee.
  - `order_total` (`NUMERIC(12, 2)`): Financial total ($Subtotal - Discount + Tax + Shipping$).
  - `purchase_timestamp` (`TIMESTAMPTZ`): Order checkout timestamp.
  - `approved_timestamp`, `delivered_timestamp`, `estimated_delivery_timestamp` (`TIMESTAMPTZ`).
  - `created_at`, `updated_at` (`TIMESTAMPTZ`).

### 1.6 `app.order_items`
- **Grain:** One row per order line-item.
- **Columns:**
  - `order_item_id` (`UUID`, PK): Line item identifier.
  - `order_id` (`UUID`, FK -> `app.orders`): Parent order.
  - `product_id` (`UUID`, FK -> `app.products`): Ordered SKU.
  - `seller_id` (`UUID`, FK -> `app.sellers`): Fulfilling merchant.
  - `quantity` (`INTEGER`): Quantity purchased (>= 1).
  - `unit_price` (`NUMERIC(12, 2)`): Unit price at purchase.
  - `discount_amount` (`NUMERIC(12, 2)`): Item-level coupon discount.
  - `freight_amount` (`NUMERIC(12, 2)`): Item allocated shipping fee.
  - `line_total` (`NUMERIC(12, 2)`): Net line total ($Qty \times UnitPrice - Discount$).
  - `created_at`, `updated_at` (`TIMESTAMPTZ`).

### 1.7 `app.payments`
- **Grain:** One row per payment attempt.
- **Columns:**
  - `payment_id` (`UUID`, PK): Unique payment record ID.
  - `order_id` (`UUID`, FK -> `app.orders`): Order being settled.
  - `payment_method` (`VARCHAR(50)`): `credit_card`, `boleto`, `pix`, `voucher`, `debit_card`.
  - `payment_status` (`VARCHAR(50)`): `captured`, `pending`, `failed`, `refunded`.
  - `payment_amount` (`NUMERIC(12, 2)`): Transaction value.
  - `transaction_reference` (`VARCHAR(255)`): Gateway processor transaction token.
  - `payment_timestamp` (`TIMESTAMPTZ`): Processor transaction timestamp.
  - `created_at`, `updated_at` (`TIMESTAMPTZ`).

### 1.8 `app.shipments`
- **Grain:** One row per delivery parcel.
- **Columns:**
  - `shipment_id` (`UUID`, PK): Unique shipment reference.
  - `order_id` (`UUID`, FK -> `app.orders`): Order reference.
  - `seller_id` (`UUID`, FK -> `app.sellers`): Dispatching merchant.
  - `warehouse_id` (`UUID`, FK -> `app.warehouses`): Dispatch facility.
  - `carrier_name` (`VARCHAR(100)`): Logistics carrier (`FedEx`, `UPS`, `DHL`, `Correios`).
  - `tracking_number` (`VARCHAR(100)`): Tracking ID.
  - `shipment_status` (`VARCHAR(50)`): `delivered`, `in_transit`, `delayed`, `failed`.
  - `dispatched_at`, `delivered_at`, `estimated_delivery_at` (`TIMESTAMPTZ`).
  - `created_at`, `updated_at` (`TIMESTAMPTZ`).

### 1.9 `app.inventory`
- **Grain:** One row per product per warehouse.
- **Columns:**
  - `inventory_id` (`UUID`, PK): Inventory record ID.
  - `product_id` (`UUID`, FK -> `app.products`).
  - `warehouse_id` (`UUID`, FK -> `app.warehouses`).
  - `quantity_on_hand` (`INTEGER`): Physical stock present in warehouse.
  - `quantity_reserved` (`INTEGER`): Stock allocated to pending orders.
  - `reorder_threshold` (`INTEGER`): Minimum inventory trigger for restocking.
  - `created_at`, `updated_at` (`TIMESTAMPTZ`).

### 1.10 `app.web_events`
- **Grain:** One row per user interaction event.
- **Columns:**
  - `event_id` (`UUID`, PK): Event ID.
  - `session_id` (`VARCHAR(100)`): Session tracking ID.
  - `customer_id` (`UUID`, Nullable FK): Authenticated customer (if logged in).
  - `product_id` (`UUID`, Nullable FK): Interacted SKU.
  - `order_id` (`UUID`, Nullable FK): Converted order.
  - `event_type` (`VARCHAR(50)`): `page_view`, `product_view`, `add_to_cart`, `checkout_start`, `purchase`.
  - `event_timestamp` (`TIMESTAMPTZ`): Timestamp of event.
  - `ip_address`, `user_agent`, `referrer_url` (`VARCHAR`).
  - `created_at` (`TIMESTAMPTZ`).

---

## 2. Operations & Observability Schema (`ops`)

Tracks orchestration states, high-watermarks, and operational run telemetry.

### 2.1 `ops.pipeline_watermarks`
- `pipeline_name` (`VARCHAR(100)`): Identifier of the pipeline (e.g. `orderflow_ingestion`).
- `source_table` (`VARCHAR(100)`): Table tracked.
- `last_successful_watermark` (`TIMESTAMPTZ`): High-watermark timestamp of last successfully extracted record.
- `last_successful_run_id` (`VARCHAR(100)`): Orchestration run ID.
- `updated_at` (`TIMESTAMPTZ`): Update timestamp.
- **PK:** `(pipeline_name, source_table)`.

### 2.2 `ops.pipeline_runs`
- `run_id` (`VARCHAR(100)`, PK): Orchestration execution UUID.
- `pipeline_name` (`VARCHAR(100)`): Pipeline identifier.
- `started_at`, `completed_at` (`TIMESTAMPTZ`).
- `status` (`VARCHAR(50)`): `RUNNING`, `SUCCESS`, `FAILED`.
- `rows_extracted`, `bytes_written` (`BIGINT`).
- `error_message` (`TEXT`).

---

## 3. Analytics Serving Schema (`analytics`)

The dimensional star schema and business data marts published from Gold Lakehouse datasets.

### 3.1 Dimensions
- `dim_customer`: Current customer attributes with surrogate key `customer_sk`.
- `dim_customer_history`: Full SCD Type 2 dimension tracking address/segment changes with `effective_from`, `effective_to`, and `is_current`.
- `dim_product`, `dim_seller`, `dim_warehouse`, `dim_date`, `dim_payment_method`, `dim_channel`.

### 3.2 Fact Tables
- `fact_orders`: Transactional order grain facts with customer SK, order total, taxes, discounts, item counts.
- `fact_order_items`: Line-item grain with margin and freight allocations.
- `fact_payments`: Financial transactions and gateway capture rates.
- `fact_shipments`: Carrier performance and delivery SLA measurements.
- `fact_inventory_daily`: Daily stock level snapshots.
- `fact_web_sessions`: Session duration and funnel stage aggregations.

### 3.3 Business Data Marts
- `mart_daily_sales`: Daily executive revenue, volumes, discounts, and margins.
- `mart_customer_cohorts`: Customer cohort retention and lifetime value.
- `mart_conversion_funnel`: Conversion drop-off across funnel milestones.
- `mart_product_performance`: Top-selling and high-margin product performance.
- `mart_delivery_performance`: Carrier on-time fulfillment rates and shipping SLA compliance.
- `mart_inventory_health`: Stockout alerts, days of inventory supply, and restock triggers.
- `audit_data_quality`: Real-time data quality check execution audit log.
