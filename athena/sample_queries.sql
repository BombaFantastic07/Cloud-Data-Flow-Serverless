-- Replace analytics_db.curated_orders with your Glue Data Catalog database/table.
-- The table should point to s3://YOUR-CURATED-BUCKET/curated/orders/
-- and expose the transformed columns, including order_year/order_month.

-- 1. Revenue and order count by status
SELECT
    status,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM analytics_db.curated_orders
GROUP BY status
ORDER BY total_amount DESC;

-- 2. Monthly revenue
SELECT
    order_year,
    order_month,
    COUNT(*) AS order_count,
    SUM(amount) AS monthly_revenue
FROM analytics_db.curated_orders
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

-- 3. Top customers by spend
SELECT
    customer_id,
    SUM(amount) AS total_spend,
    COUNT(*) AS orders
FROM analytics_db.curated_orders
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;
