-- anomalies.sql
WITH daily_sales AS (
    SELECT 
        DATE_TRUNC('day', ORDER_DATE) AS order_day,
        SUM(ORDER_AMOUNT) AS daily_revenue
    FROM ANALYTICS.FACT_ORDERS_CLEAN
    GROUP BY 1
),
rolling_stats AS (
    SELECT 
        order_day,
        daily_revenue,
        AVG(daily_revenue) OVER (ORDER BY order_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_7d,
        STDDEV(daily_revenue) OVER (ORDER BY order_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_std_7d
    FROM daily_sales
)
SELECT 
    order_day,
    daily_revenue,
    rolling_avg_7d,
    rolling_std_7d,
    CASE 
        WHEN ABS(daily_revenue - rolling_avg_7d) > (2 * rolling_std_7d) THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS status  
FROM rolling_stats