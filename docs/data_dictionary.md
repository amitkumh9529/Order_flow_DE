# 📖 OrderFlow Data Dictionary

This data dictionary defines all entities, grains, attributes, constraints, and business logic across the OrderFlow enterprise platform.

---

## 1. Dimensional Models (`analytics.dim_*`)

### 1.1 `dim_customer`
- **Grain:** One record per customer (Current state).
- **Primary Key:** `customer_sk` (Surrogate Key) / `customer_id` (Natural Key).

| Column Name | Data Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- |
| `customer_sk` | `BIGINT` | No | Surrogate integer primary key. |
| `customer_id` | `VARCHAR(64)` | No | Natural UUID of customer. |
| `first_name` | `VARCHAR(100)` | No | Customer given name (trimmed). |
| `last_name` | `VARCHAR(100)` | No | Customer family name (trimmed). |
| `email` | `VARCHAR(255)` | No | Lowercased, sanitized email address. |
| `city` | `VARCHAR(100)` | Yes | Residential city. |
| `state` | `VARCHAR(100)` | Yes | Residential state code. |
| `postal_prefix` | `VARCHAR(20)` | Yes | Postal code prefix for geo routing. |
| `customer_segment` | `VARCHAR(50)` | No | Segment: `VIP`, `Loyal`, `Standard`, `Occasional`. |
| `registration_date`| `DATE` | No | Date of initial account signup. |

### 1.2 `dim_customer_history` (SCD Type 2)
- **Grain:** One record per customer per historical version.
- **Primary Key:** `customer_history_sk`.

| Column Name | Data Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- |
| `customer_history_sk` | `BIGINT` | No | Surrogate primary key for SCD2 record. |
| `customer_id` | `VARCHAR(64)` | No | Natural customer UUID. |
| `effective_from` | `TIMESTAMPTZ` | No | Point in time this version became active. |
| `effective_to` | `TIMESTAMPTZ` | No | Superseded timestamp (9999-12-31 if current). |
| `is_current` | `BOOLEAN` | No | `TRUE` if currently active version, `FALSE` otherwise. |

### 1.3 `dim_product`
- **Grain:** One record per unique product SKU.

| Column Name | Data Type | Nullable | Description / Business Rules |
| :--- | :--- | :--- | :--- |
| `product_id` | `VARCHAR(64)` | No | SKU natural identifier. |
| `product_name` | `VARCHAR(255)` | No | Standardized product catalog name. |
| `category` | `VARCHAR(100)` | No | Primary product category. |
| `subcategory` | `VARCHAR(100)` | Yes | Sub-classification. |
| `brand` | `VARCHAR(100)` | Yes | Brand or manufacturer name. |
| `unit_price` | `NUMERIC(12, 2)` | No | Standard catalog sales price. |
| `cost_price` | `NUMERIC(12, 2)` | No | Cost of goods sold (COGS). |
| `weight_kg` | `NUMERIC(8, 3)` | No | Shipping weight in kg. |
| `is_active` | `BOOLEAN` | No | Catalog listing status. |

---

## 2. Fact Tables (`analytics.fact_*`)

### 2.1 `fact_orders`
- **Grain:** One record per placed customer order.
- **Primary Key:** `order_id`.

| Column Name | Data Type | Nullable | Formula / Description |
| :--- | :--- | :--- | :--- |
| `order_id` | `VARCHAR(64)` | No | Unique order UUID. |
| `customer_id` | `VARCHAR(64)` | No | Placing customer foreign key. |
| `order_status` | `VARCHAR(50)` | No | `delivered`, `shipped`, `in_transit`, `processing`, `cancelled`. |
| `order_channel` | `VARCHAR(50)` | No | Order placement channel (`web`, `mobile_app`, `marketplace`). |
| `subtotal` | `NUMERIC(12, 2)` | No | $\sum (\text{item line totals})$. |
| `discount_amount`| `NUMERIC(12, 2)` | No | Promo/coupon deduction. |
| `tax_amount` | `NUMERIC(12, 2)` | No | Sales tax (18% standard rate). |
| `shipping_amount`| `NUMERIC(12, 2)` | No | Freight shipping fee. |
| `order_total` | `NUMERIC(12, 2)` | No | $\text{Subtotal} - \text{Discount} + \text{Tax} + \text{Shipping}$. |
| `purchase_timestamp` | `TIMESTAMPTZ` | No | Timestamp of order checkout. |

### 2.2 `fact_order_items`
- **Grain:** One record per order line item.

| Column Name | Data Type | Nullable | Formula / Description |
| :--- | :--- | :--- | :--- |
| `order_item_id` | `VARCHAR(64)` | No | Unique line item UUID. |
| `order_id` | `VARCHAR(64)` | No | Parent order FK. |
| `product_id` | `VARCHAR(64)` | No | Purchased product FK. |
| `seller_id` | `VARCHAR(64)` | No | Fulfilling seller FK. |
| `quantity` | `INTEGER` | No | Units purchased (>= 1). |
| `unit_price` | `NUMERIC(12, 2)` | No | Price per unit at purchase. |
| `line_total` | `NUMERIC(12, 2)` | No | $\text{Quantity} \times \text{UnitPrice} - \text{Discount}$. |

---

## 3. Business Data Marts (`analytics.mart_*`)

| Mart Name | Grain | Key Metrics & Dimensions |
| :--- | :--- | :--- |
| `mart_daily_sales` | Date + Category + Channel | `gross_revenue`, `order_count`, `avg_order_value`, `total_units`, `discount_rate`. |
| `mart_customer_cohorts` | Acquisition Month + Active Month | `cohort_size`, `active_customers`, `repeat_order_count`, `retention_rate_pct`. |
| `mart_conversion_funnel`| Date + Device + Channel | `sessions`, `product_views`, `add_to_carts`, `checkouts`, `purchases`, `cart_to_order_pct`. |
| `mart_product_performance`| Product SKU | `units_sold`, `gross_revenue`, `gross_margin_dollars`, `return_rate_pct`. |
| `mart_delivery_performance`| Carrier + Month | `total_shipments`, `avg_transit_days`, `on_time_deliveries`, `on_time_pct`. |
| `mart_inventory_health` | Warehouse + Category | `total_skus`, `low_stock_skus`, `stockout_skus`, `stockout_risk_score`. |
| `audit_data_quality` | Check Execution | `check_name`, `records_checked`, `records_failed`, `check_status`, `failure_percentage`. |
