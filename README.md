# E-Commerce OLTP Faker System

A fake data generation system for an e-commerce OLTP model on **PostgreSQL**, including a Python script that automatically creates master data (via Faker), high-volume transactional data (Orders/Items), and dynamic reporting functions.

---

### File Descriptions

| File                      | Description                                                                                                                |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `sql/init_tables.sql`     | Creates the full schema: `brand`, `category`, `seller`, `product`, `promotion`, `order`, `order_item`, `promotion_product` |
| `sql/query/` | Contains raw SQL scripts for analysis and snapshot reporting (e.g., monthly revenue, top-selling products, order filtering)
| `sql/dynamic_report/`     | PL/pgSQL dynamic reporting functions (revenue, seller performance, top products)                                           |
| `src/master_data.py`      | Generates fake data (Master Data) and inserts into PostgreSQL                                                              |
| `src/transaction_data.py` | Generates Transaction Data and bulk-inserts it into PostgreSQL                                                             |
| `src/config.py`           | DB connection settings, random seed, and data volume control parameters (e.g. 2.5 million orders)                          |
| `src/database.py`         | `connect()` helper for PostgreSQL via `psycopg2`                                                                           |

---

## ⚙️ Requirements

- **PostgreSQL** (tested with `psql 16.x`)
- **Python** `3.12`
- **Poetry** (tested with `Poetry 1.8.x`)

---

## Quickstart

### 1. Install dependencies

```bash
poetry install
```

### 2. Configure the database connection

Update `DB_CONFIG` in `src/config.py` to match your environment.

Default configuration:

```python
DB_CONFIG = {
    "host":     "localhost",
    "user":     "postgres",
    "password": "170723",
    "database": "ecommerce_oltp",
    "port":     5432,
}
```

### 3. Create the database and schema

> **Warning:** `sql/init_tables.sql` contains `DROP TABLE IF EXISTS ...` statements — all existing data will be erased when re-run.

```bash
# Create the database (if it does not exist)
psql -U postgres -c "CREATE DATABASE ecommerce_oltp;"

# Create tables
psql -U postgres -d ecommerce_oltp -f sql/init_tables.sql
```

### 4. Generate and load data into PostgreSQL

Run from the project root directory:

```bash
# Generate master data
poetry run python src/master_data.py

# Generate transaction data
poetry run python src/transaction_data.py
```

This will automatically generate and bulk-insert millions of rows depending on the configuration in `config.py`.

---

## Generated Data

`src/master_data.py` and `src/transaction_data.py` produces two categories of data:

### Master Data

| Table | Description |
|-------|-------------|
| `brand` | Product brands |
| `category` | Level-1 and level-2 categories (parent–child relationship) |
| `seller` | Sellers / vendors |
| `promotion` | Promotional campaigns |
| `promotion_product` | Promotion ↔ product mapping |
| `product` | Products — `discount_price` is computed from the best applicable promotion, with a minimum floor of 10% of the original price |

### Transaction Data (High Volume)

| Table | Description |
|-------|-------------|
| `order` | Millions of orders distributed across a pre-configured date range |
| `order_item` | 2–4 randomly selected products per order, linked to products with subtotal calculations |

---

## Querying & Dynamic Reports

Once data has been loaded, use the built-in functions to generate business insights. All functions support optional filter parameters (date ranges, seller lists, product lists, etc.).

### 1. Monthly Revenue Report

```sql
SELECT * FROM report_monthly_revenue('2025-08-01', '2025-10-31');
```

### 2. Daily Revenue Report (filtered by Product IDs)

```sql
SELECT * FROM report_daily_revenue('2025-08-01', '2025-10-31', ARRAY[10, 25, 40]);
```

### 3. Seller Performance (filtered by Brand ID)

```sql
-- Signature: (start_date, end_date, category_id, brand_id)
-- Pass NULL to skip any individual filter
SELECT * FROM report_seller_performance('2025-08-01', '2025-10-31', NULL, 5);
```

### 4. Top Products per Brand (filtered by Seller IDs)

```sql
SELECT * FROM report_top_products_per_brand('2025-08-01', '2025-10-31', ARRAY[1, 2, 3]);
```

### 5. Order Status Summary

```sql
SELECT * FROM report_orders_status_summary('2025-08-01', '2025-10-31', NULL, NULL);
```

---

## Tuning Data Volume

Edit the parameters in `src/config.py`:

| Parameter | Description |
|-----------|-------------|
| `SEED` | Random seed for reproducibility |
| `DATA_VOLUME` | Number of records per table (e.g. increase `orders` to `5_000_000` for heavy load testing) |
| `CATEGORY_MAP` | Sample pool for category generation |
| `PROMO_NAMES` | Sample promotion name pool |
| `PROMO_TYPES` | Sample promotion type pool |
