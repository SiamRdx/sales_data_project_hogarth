{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- h. Monthly, Yearly & Quarterly comparisons for Revenue and Growth
-- yearly sales growth
-- Considerations: Immediate previous year comparison


with yearly_sales AS (
SELECT 
 b.ytd_start as year
, SUM(IFNULL(a.revenue,0)) AS revenue

 FROM 
{{ ref("fact_sales") }} a  
JOIN {{ ref("dim_date") }} b
ON a.dim_order_date_id = b.dim_date_id
GROUP BY b.ytd_start ) 

, yearly_sales_prev AS (
SELECT
   year
  , revenue AS current_revenue
  , LAG(revenue,1) OVER (ORDER BY year) AS prev_revenue
  FROM yearly_sales
)

, yearly_sales_growth AS (
SELECT
year
, current_revenue
, CASE WHEN prev_revenue IS NULL THEN 0
       WHEN prev_revenue = 0 THEN 0
       ELSE ( (current_revenue - prev_revenue)/prev_revenue ) * 100 END AS sales_growth
FROM yearly_sales_prev
)

SELECT * FROM yearly_sales_growth