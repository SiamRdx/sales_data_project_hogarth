{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- b. Region & City ranking by revenue, quantity & Sales
-- city ranking

WITH city_sales AS (
SELECT 
  b.city
, SUM(a.revenue) AS total_revenue
, SUM(a.quantity) AS total_quantity

FROM
{{ref("fact_sales")}} a 
JOIN {{ref("dim_region")}} b
ON a.dim_region_id = b.dim_region_id
GROUP BY 1)

SELECT 

  city
, total_revenue
, RANK() OVER(ORDER BY total_revenue DESC) rank_by_revenue
, total_quantity
, RANK() OVER(ORDER BY total_quantity DESC) rank_by_quantity
FROM city_sales