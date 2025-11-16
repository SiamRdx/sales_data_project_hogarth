{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- b. Region & City ranking by revenue, quantity & Sales
-- region ranking

WITH region_sales AS (
SELECT 
  b.region
, SUM(a.revenue) AS total_revenue
, SUM(a.quantity) AS total_quantity

FROM
{{ref("fact_sales")}} a 
JOIN {{ref("dim_salesperson")}} b
ON a.dim_salesperson_id = b.dim_salesperson_id
GROUP BY 1)

SELECT 

  region
, total_revenue
, RANK() OVER(ORDER BY total_revenue DESC) rank_by_revenue
, total_quantity
, RANK() OVER(ORDER BY total_quantity DESC) rank_by_quantity
FROM region_sales