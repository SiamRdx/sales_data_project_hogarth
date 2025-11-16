{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- h. Monthly, Yearly & Quarterly comparisons for Revenue and Growth
-- monthly sales growth
-- Considerations: Immediate previous month comparison

with monthly_sales AS (
SELECT 
 b.mtd_start as month
, SUM(IFNULL(a.revenue,0)) AS revenue

 FROM 
{{ ref("fact_sales") }} a  
JOIN {{ ref("dim_date") }} b
ON a.dim_order_date_id = b.dim_date_id
GROUP BY b.mtd_start ) 

, monthy_sales_prev AS (
SELECT
   month
  , revenue AS current_revenue
  , LAG(revenue,1) OVER (ORDER BY month) AS prev_revenue
  FROM monthly_sales
)


, monthly_sales_growth AS (
SELECT
month
, current_revenue
, CASE WHEN prev_revenue IS NULL THEN 0
       WHEN prev_revenue = 0 THEN 0
       ELSE ( (current_revenue - prev_revenue)/prev_revenue ) * 100 END AS sales_growth
FROM monthy_sales_prev
)

SELECT * FROM monthly_sales_growth