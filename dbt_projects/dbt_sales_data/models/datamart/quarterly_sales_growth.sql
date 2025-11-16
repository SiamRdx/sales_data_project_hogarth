{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}


-- h. Monthly, Yearly & Quarterly comparisons for Revenue and Growth
-- quarterly sales growth
-- Considerations: Immediate previous quarter comparison


with quarterly_sales AS (
SELECT 
 b.qtd_start as quarter
, SUM(IFNULL(a.revenue,0)) AS revenue

 FROM 
{{ ref("fact_sales") }} a  
JOIN {{ ref("dim_date") }} b
ON a.dim_order_date_id = b.dim_date_id
GROUP BY b.qtd_start ) 

, quarterly_sales_prev AS (
SELECT
   quarter
  , revenue AS current_revenue
  , LAG(revenue,1) OVER (ORDER BY quarter) AS prev_revenue
  FROM quarterly_sales
)


, quarterly_sales_growth AS (
SELECT
quarter
, current_revenue
, CASE WHEN prev_revenue IS NULL THEN 0
       WHEN prev_revenue = 0 THEN 0
       ELSE ( (current_revenue - prev_revenue)/prev_revenue ) * 100 END AS sales_growth
FROM quarterly_sales_prev
)

SELECT * FROM quarterly_sales_growth