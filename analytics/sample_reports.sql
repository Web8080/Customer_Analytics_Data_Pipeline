-- ======================================================================
-- STEP 1 — DATA COLLECTION / SIMULATION (RAW LAYER)  ✅
-- ======================================================================

-- 0) Warehouse / DB / Schemas
CREATE WAREHOUSE IF NOT EXISTS my_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
USE WAREHOUSE my_wh;

CREATE DATABASE IF NOT EXISTS my_project;
USE DATABASE my_project;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- 1) Stage + CSV File Format (comma-delimited)
CREATE OR REPLACE STAGE my_stage;

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  NULL_IF = ('NULL','null','');

-- 2) RAW Tables (match your CSVs exactly)
USE SCHEMA RAW;

-- 
CREATE OR REPLACE TABLE raw_customers (
  customer_id VARCHAR(50),
  first_name  VARCHAR(50),
  last_name   VARCHAR(50),
  email       VARCHAR(100),
  phone       VARCHAR(50),
  country     VARCHAR(50)
);

CREATE OR REPLACE TABLE raw_products (
  product_id     VARCHAR(50),
  product_name   VARCHAR(150),
  category       VARCHAR(100),
  price          NUMBER(10,2),
  currency       VARCHAR(10),
  stock_quantity NUMBER
);

CREATE OR REPLACE TABLE raw_orders (
  order_id     VARCHAR(50),
  customer_id  VARCHAR(50),
  product_id   VARCHAR(50),
  order_date   VARCHAR(25),     -- load as text first; cast later
  quantity     NUMBER,
  total_amount NUMBER(10,2),
  currency     VARCHAR(10)
);

CREATE OR REPLACE TABLE raw_clicks (
  click_id        VARCHAR(50),
  customer_id     VARCHAR(50),
  product_id      VARCHAR(50),
  page_url        VARCHAR(500),
  click_timestamp VARCHAR(50),  -- load as text first; cast later
  device_type     VARCHAR(50)
);

CREATE OR REPLACE TABLE raw_support_tickets (
  ticket_id             VARCHAR(50),
  customer_id           VARCHAR(50),
  issue_category        VARCHAR(100),
  created_at            VARCHAR(50),  -- load as text first; cast later
  status                VARCHAR(50),
  resolution_time_hours VARCHAR(50)   -- load as text; cast later
);

-- 3) COPY INTO (robust posture)
-- ============================================
-- CLEANUP OLD FILES FROM STAGE
-- ============================================

-- List files first (to confirm what’s inside)
--LIST @my_stage;

-- Remove specific files
--REMOVE @my_stage/raw_customers.csv;
--REMOVE @my_stage/raw_products.csv;
--REMOVE @my_stage/raw_orders.csv;
--REMOVE @my_stage/raw_clicks.csv;
--REMOVE @my_stage/raw_support_tickets.csv;

-- Or remove everything from the stage at once
--REMOVE @my_stage;

-- Upload files to @my_stage first (Snowflake UI > Load or PUT). Filenames must match.
COPY INTO raw_customers
FROM @my_stage/raw_customers.csv
FILE_FORMAT=(FORMAT_NAME=my_csv_format)
ON_ERROR='CONTINUE';

COPY INTO raw_products
FROM @my_stage/raw_products.csv
FILE_FORMAT=(FORMAT_NAME=my_csv_format)
ON_ERROR='CONTINUE';

COPY INTO raw_orders
FROM @my_stage/raw_orders.csv
FILE_FORMAT=(FORMAT_NAME=my_csv_format)
ON_ERROR='CONTINUE';

COPY INTO raw_clicks
FROM @my_stage/raw_clicks.csv
FILE_FORMAT=(FORMAT_NAME=my_csv_format)
ON_ERROR='CONTINUE';

COPY INTO raw_support_tickets
FROM @my_stage/raw_support_tickets.csv
FILE_FORMAT=(FORMAT_NAME=my_csv_format)
ON_ERROR='CONTINUE';

-- 4) Quick sanity checks
SELECT 'raw_customers' tbl, COUNT(*) cnt FROM raw_customers
UNION ALL SELECT 'raw_products', COUNT(*) FROM raw_products
UNION ALL SELECT 'raw_orders', COUNT(*) FROM raw_orders
UNION ALL SELECT 'raw_clicks', COUNT(*) FROM raw_clicks
UNION ALL SELECT 'raw_support_tickets', COUNT(*) FROM raw_support_tickets;

-- Optional: inspect a few rows
SELECT * FROM raw_customers LIMIT 5;
SELECT * FROM raw_products LIMIT 5;

-- Check for missing IDs or NULLs
SELECT COUNT(*) AS missing_customer_id FROM raw_customers WHERE customer_id IS NULL;
SELECT COUNT(*) AS missing_product_id FROM raw_products WHERE product_id IS NULL;
SELECT COUNT(*) AS missing_order_id FROM raw_orders WHERE order_id IS NULL;

-- Check for consistency between orders and products/customers
SELECT COUNT(*) AS missing_customers_in_orders
FROM raw_orders o
LEFT JOIN raw_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT COUNT(*) AS missing_products_in_orders
FROM raw_orders o
LEFT JOIN raw_products p ON o.product_id = p.product_id
WHERE p.product_id IS NULL;


-- ======================================================================
-- STEP 2 — DATA MODELING (STAR SCHEMA in ANALYTICS)  ✅
--   Dim_Customers, Dim_Products, Dim_Date, Fact_Orders
--   Cast types cleanly, enforce consistent PK/FK types (VARCHAR(50))
-- ======================================================================
USE SCHEMA ANALYTICS;

-- 2.1) Dimension: Customers
CREATE OR REPLACE TABLE dim_customers AS
SELECT
  TRIM(customer_id) AS customer_id,
  TRIM(first_name)  AS first_name,
  TRIM(last_name)   AS last_name,
  TRIM(email)       AS email,
  TRIM(phone)       AS phone,
  TRIM(country)     AS country
FROM RAW.raw_customers;

-- Add PK (Snowflake relies on declaration; not enforced physically, but useful)
ALTER TABLE dim_customers ADD CONSTRAINT pk_dim_customers PRIMARY KEY (customer_id);

-- 2.2) Dimension: Products
CREATE OR REPLACE TABLE dim_products AS
SELECT
  TRIM(product_id)   AS product_id,
  TRIM(product_name) AS product_name,
  TRIM(category)     AS category,
  TRY_TO_DECIMAL(price,10,2) AS price,
  TRIM(currency)     AS currency,
  TRY_TO_NUMBER(stock_quantity) AS stock_quantity
FROM RAW.raw_products;

ALTER TABLE dim_products ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_id);

-- 2.3) Dimension: Date
-- Build a date dimension from the min/max order_date in raw_orders (flexible if you add data).
-- First: parse order_date safely (support YYYY-MM-DD or other common formats)
CREATE OR REPLACE TABLE dim_date AS
WITH parsed AS (
  SELECT DISTINCT
    COALESCE(
      TRY_TO_DATE(order_date, 'YYYY-MM-DD'),
      TRY_TO_DATE(order_date, 'YYYY/MM/DD'),
      TRY_TO_DATE(order_date, 'MM/DD/YYYY'),
      TRY_TO_DATE(order_date)  -- best-effort
    ) AS dt
  FROM RAW.raw_orders
),
bounds AS (
  SELECT
    COALESCE(MIN(dt), CURRENT_DATE() - 365) AS mind,
    COALESCE(MAX(dt), CURRENT_DATE() + 365) AS maxd
  FROM parsed
),
gen AS (
  SELECT DATEADD(DAY, seq4(), mind) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 10000)) g, bounds
  WHERE DATEADD(DAY, seq4(), mind) <= maxd
)
SELECT
  d::DATE                                  AS date_key,
  YEAR(d)                                  AS year,
  LPAD(MONTH(d)::STRING,2,'0')             AS month_num,
  MONTHNAME(d)                              AS month_name,
  QUARTER(d)                                AS quarter,
  WEEKOFYEAR(d)                             AS week_of_year,
  DAY(d)                                    AS day_of_month,
  TO_CHAR(d,'YYYY-MM-DD')                   AS date_iso
FROM gen
ORDER BY date_key;

ALTER TABLE dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key);

-- 2.4) Fact: Orders (type-clean and link to dims)
CREATE OR REPLACE TABLE fact_orders AS
WITH ord AS (
  SELECT
    TRIM(order_id)               AS order_id,
    TRIM(customer_id)            AS customer_id,
    TRIM(product_id)             AS product_id,
    COALESCE(
      TRY_TO_DATE(order_date,'YYYY-MM-DD'),
      TRY_TO_DATE(order_date,'YYYY/MM/DD'),
      TRY_TO_DATE(order_date,'MM/DD/YYYY'),
      TRY_TO_DATE(order_date)
    )                            AS order_date,
    TRY_TO_NUMBER(quantity)      AS quantity,
    TRY_TO_DECIMAL(total_amount,10,2) AS total_amount,
    TRIM(currency)               AS currency
  FROM RAW.raw_orders
  WHERE order_id IS NOT NULL
)
SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  o.order_date,
  o.quantity,
  o.total_amount,
  o.currency
FROM ord o
-- keep only records with valid date & positive quantity
WHERE o.order_date IS NOT NULL
  AND o.quantity >= 0;

-- Add PK/FK declarations (make sure types match: all VARCHAR(50) for IDs)
ALTER TABLE fact_orders ADD CONSTRAINT pk_fact_orders PRIMARY KEY (order_id);
ALTER TABLE fact_orders ADD CONSTRAINT fk_fact_cust FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id);
ALTER TABLE fact_orders ADD CONSTRAINT fk_fact_prod FOREIGN KEY (product_id) REFERENCES dim_products(product_id);

-- 2.5) Quick star checks
SELECT 'dim_customers' t, COUNT(*) c FROM dim_customers
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_orders', COUNT(*) FROM fact_orders;

-- 2.6) Simple analytic sanity queries
-- Top customers by spend (joined)
SELECT d.first_name || ' ' || d.last_name AS customer_name,
       SUM(f.total_amount) AS total_spent,
       COUNT(*) AS orders_cnt
FROM fact_orders f
JOIN dim_customers d ON f.customer_id = d.customer_id
GROUP BY 1
ORDER BY total_spent DESC
LIMIT 10;

-- Revenue by month
SELECT dd.year, dd.month_num, dd.month_name,
       SUM(f.total_amount) AS revenue
FROM fact_orders f
JOIN dim_date dd ON f.order_date = dd.date_key
GROUP BY 1,2,3
ORDER BY 1,2;


-- ===============================
-- 1️⃣ Data Sanity Checks & Enrichment
-- ===============================

-- 1.1 Count rows per table
SELECT 'raw_customers' AS table_name, COUNT(*) AS total_rows FROM RAW.raw_customers
UNION ALL
SELECT 'raw_products', COUNT(*) FROM RAW.raw_products
UNION ALL
SELECT 'raw_orders', COUNT(*) FROM RAW.raw_orders
UNION ALL
SELECT 'raw_clicks', COUNT(*) FROM RAW.raw_clicks
UNION ALL
SELECT 'raw_support_tickets', COUNT(*) FROM RAW.raw_support_tickets;

-- 1.2 Check for missing foreign keys in fact_orders
SELECT COUNT(*) AS missing_customers
FROM RAW.raw_orders o
LEFT JOIN RAW.raw_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT COUNT(*) AS missing_products
FROM RAW.raw_orders o
LEFT JOIN RAW.raw_products p ON o.product_id = p.product_id
WHERE p.product_id IS NULL;

-- 1.3 Data profiling for numeric columns
-- Products
SELECT
    COUNT(*) AS total_products,
    COUNT(DISTINCT product_id) AS unique_products,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price
FROM RAW.raw_products;

-- Orders
SELECT
    COUNT(*) AS total_orders,
    MIN(quantity) AS min_qty,
    MAX(quantity) AS max_qty,
    AVG(quantity) AS avg_qty,
    MIN(total_amount) AS min_amount,
    MAX(total_amount) AS max_amount,
    AVG(total_amount) AS avg_amount
FROM RAW.raw_orders;

-- 1.4 Enrich fact_orders with calculated column
CREATE OR REPLACE TABLE analytics_orders_enriched AS
SELECT
    o.*,
    p.price,
    o.quantity * p.price AS order_amount
FROM RAW.raw_orders o
LEFT JOIN RAW.raw_products p
    ON o.product_id = p.product_id;

-- Optional: check a few rows
SELECT * FROM analytics_orders_enriched LIMIT 5;


-- 1.5 Categorize products by revenue bucket (high/medium/low)
CREATE OR REPLACE TABLE analytics_product_summary AS
SELECT
    p.*,
    CASE
        WHEN p.price >= 100 THEN 'High'
        WHEN p.price >= 50 THEN 'Medium'
        ELSE 'Low'
    END AS revenue_bucket
FROM RAW.raw_products p;

-- Optional: Check enriched tables
SELECT * FROM analytics_orders_enriched LIMIT 5;
SELECT * FROM analytics_product_summary LIMIT 5;






-- 1️⃣ Ensure all enriched/analytics tables exist
CREATE OR REPLACE TABLE ANALYTICS.analytics_customers AS
SELECT * FROM RAW.raw_customers;

CREATE OR REPLACE TABLE ANALYTICS.analytics_products AS
SELECT * FROM RAW.raw_products;

CREATE OR REPLACE TABLE ANALYTICS.analytics_orders AS
SELECT * FROM analytics_orders_enriched;

CREATE OR REPLACE TABLE ANALYTICS.analytics_clicks AS
SELECT * FROM RAW.raw_clicks;

CREATE OR REPLACE TABLE ANALYTICS.analytics_support_tickets AS
SELECT * FROM RAW.raw_support_tickets;

-- 2️⃣ Optional: create views for DBT consumption
CREATE OR REPLACE VIEW ANALYTICS.vw_orders AS
SELECT * FROM ANALYTICS.analytics_orders;

CREATE OR REPLACE VIEW ANALYTICS.vw_customers AS
SELECT * FROM ANALYTICS.analytics_customers;

CREATE OR REPLACE VIEW ANALYTICS.vw_products AS
SELECT * FROM ANALYTICS.analytics_products;

-- 3️⃣ Optional: clone tables as backup before DBT transformations
CREATE OR REPLACE TABLE ANALYTICS.analytics_orders_backup CLONE ANALYTICS.analytics_orders;
CREATE OR REPLACE TABLE ANALYTICS.analytics_customers_backup CLONE ANALYTICS.analytics_customers;
CREATE OR REPLACE TABLE ANALYTICS.analytics_products_backup CLONE ANALYTICS.analytics_products;


-- ======================================================================
-- STEP 3 — DBT PROJECT (STAGING & MARTS)  ✅

-- ======================================================================

-- Create staging tables (stg_*)

-- STAGING CUSTOMERS
CREATE OR REPLACE TABLE staging_customers AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    country
FROM analytics_customers;

-- STAGING PRODUCTS
CREATE OR REPLACE TABLE staging_products AS
SELECT *
FROM analytics_products;

-- STAGING ORDERS
CREATE OR REPLACE TABLE staging_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    price,
    order_date,
    quantity * price AS order_amount
FROM analytics_orders;

-- STAGING CLICKS
CREATE OR REPLACE TABLE staging_clicks AS
SELECT *
FROM analytics_clicks;

-- STAGING SUPPORT TICKETS
CREATE OR REPLACE TABLE staging_support_tickets AS
SELECT *
FROM analytics_support_tickets;


--2) Create fact and dimension tables (marts / star schema)

-- DIM CUSTOMERS
CREATE OR REPLACE TABLE dim_customers AS
SELECT *
FROM staging_customers;

-- DIM PRODUCTS
CREATE OR REPLACE TABLE dim_products AS
SELECT *
FROM staging_products;

-- DIM DATE
CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    TO_DATE(order_date, 'YYYY-MM-DD') AS date_key,
    YEAR(TO_DATE(order_date, 'YYYY-MM-DD')) AS year,
    LPAD(MONTH(TO_DATE(order_date, 'YYYY-MM-DD'))::STRING, 2, '0') AS month_num,
    MONTHNAME(TO_DATE(order_date, 'YYYY-MM-DD')) AS month_name,
    QUARTER(TO_DATE(order_date, 'YYYY-MM-DD')) AS quarter,
    WEEKOFYEAR(TO_DATE(order_date, 'YYYY-MM-DD')) AS week_of_year,
    DAY(TO_DATE(order_date, 'YYYY-MM-DD')) AS day_of_month,
    TO_CHAR(TO_DATE(order_date, 'YYYY-MM-DD'),'YYYY-MM-DD') AS date_iso
FROM staging_orders;


-- FACT ORDERS
CREATE OR REPLACE TABLE fact_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.price,
    o.order_amount,
    o.order_date
FROM staging_orders o;


-- 3) Validation Queries
-- Check row counts:
SELECT COUNT(*) AS total_customers FROM dim_customers;
SELECT COUNT(*) AS total_products FROM dim_products;
SELECT COUNT(*) AS total_orders FROM fact_orders;
SELECT COUNT(*) AS total_dates FROM dim_date;

-- Check for null foreign keys
SELECT COUNT(*) AS missing_customers
FROM fact_orders
WHERE customer_id IS NULL;

SELECT COUNT(*) AS missing_products
FROM fact_orders
WHERE product_id IS NULL;


----- rough work
SELECT * 
FROM ANALYTICS.dim_customers;

select * from RAW.raw_customers


----------------------
--identify metrics/kpis
------------------

-- Total customers by country


---New customers over time
CREATE table analytics.new_customers_daily AS
SELECT signup_date, COUNT(*) AS new_customers
FROM analytics.customers
GROUP BY signup_date
ORDER BY signup_date;



-------------------

---- Top 10 Customers by Revenue
CREATE OR REPLACE VIEW ANALYTICS.vw_top_customers AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    SUM(f.order_amount) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM ANALYTICS.fact_orders f
JOIN ANALYTICS.dim_customers c 
    ON f.customer_id = c.customer_id
GROUP BY 1, 2
ORDER BY total_revenue DESC
LIMIT 10;


----- Anomaly Detection View
-- I will flag days where sales deviate strongly from the rolling average (e.g., ±30%).
CREATE OR REPLACE VIEW ANALYTICS.vw_sales_anomalies AS
WITH daily_sales AS (
    SELECT 
        DATE_TRUNC('day', TRY_TO_DATE(f.order_date, 'YYYY-MM-DD')) AS order_day,
        SUM(f.order_amount) AS daily_revenue
    FROM ANALYTICS.fact_orders f
    GROUP BY 1
),
rolling_stats AS (
    SELECT 
        order_day,
        daily_revenue,
        AVG(daily_revenue) OVER (
            ORDER BY order_day 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_7d,
        STDDEV(daily_revenue) OVER (
            ORDER BY order_day 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_std_7d
    FROM daily_sales
)
SELECT 
    order_day,
    daily_revenue,
    rolling_avg_7d,
    rolling_std_7d,
    CASE 
        WHEN ABS(daily_revenue - rolling_avg_7d) > (2 * rolling_std_7d)
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS status
FROM rolling_stats;

----
SHOW TABLES IN SCHEMA MY_PROJECT.ANALYTICS;

----

DESCRIBE TABLE MY_PROJECT.ANALYTICS.FACT_ORDERS;

----
CREATE OR REPLACE TABLE MY_PROJECT.ANALYTICS.FACT_ORDERS_CLEAN AS
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    QUANTITY,
    PRICE,
    ORDER_AMOUNT,
    TO_DATE(ORDER_DATE, 'YYYY-MM-DD') AS ORDER_DATE -- adjust format if needed
FROM MY_PROJECT.ANALYTICS.FACT_ORDERS;


--- Top 10 Customers by Revenue
CREATE OR REPLACE VIEW MY_PROJECT.ANALYTICS.TOP_CUSTOMERS AS
SELECT
    CUSTOMER_ID,
    SUM(ORDER_AMOUNT) AS TOTAL_REVENUE
FROM MY_PROJECT.ANALYTICS.FACT_ORDERS_CLEAN
GROUP BY CUSTOMER_ID
ORDER BY TOTAL_REVENUE DESC
LIMIT 10;

------------------
-- Product Category Performance
CREATE OR REPLACE VIEW MY_PROJECT.ANALYTICS.PRODUCT_CATEGORY_SALES AS
SELECT
    P.CATEGORY,
    SUM(F.ORDER_AMOUNT) AS CATEGORY_REVENUE
FROM MY_PROJECT.ANALYTICS.FACT_ORDERS_CLEAN F
JOIN MY_PROJECT.ANALYTICS.DIM_PRODUCTS P
    ON F.PRODUCT_ID = P.PRODUCT_ID
GROUP BY P.CATEGORY
ORDER BY CATEGORY_REVENUE DESC;



---
USE DATABASE MY_PROJECT;
USE SCHEMA ANALYTICS;

SHOW VIEWS;


--- check dbt models 
SELECT * FROM ANALYTICS_ANALYTICS.my_first_dbt_model;

SELECT *
FROM ANALYTICS_ANALYTICS.VW_CUSTOMERS
LIMIT 10;

SHOW TABLES IN SCHEMA ANALYTICS_ANALYTICS;
SHOW VIEWS IN SCHEMA ANALYTICS_ANALYTICS;

SELECT *
FROM analytics.vw_customers
LIMIT 10;


