{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- d. Product ranking by revenue, quantity & Sales

WITH product_sales AS (
SELECT 
  b.product_name
, SUM(a.revenue) AS total_revenue
, SUM(a.quantity) AS total_quantity

FROM
{{ref("fact_sales")}} a 
JOIN {{ref("dim_product")}} b
ON a.dim_product_id = b.dim_product_id
GROUP BY 1)

SELECT 

  product_name
, total_revenue
, RANK() OVER(ORDER BY total_revenue DESC) rank_by_revenue
, total_quantity
, RANK() OVER(ORDER BY total_quantity DESC) rank_by_quantity
FROM product_sales