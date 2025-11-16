{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- a. Customer ranking by revenue, quantity & Sales

WITH customer_sales AS (
SELECT 
  b.customer_id
, b.customer_name
, SUM(a.revenue) AS total_revenue
, SUM(a.quantity) AS total_quantity

FROM
{{ref("fact_sales")}} a 
JOIN {{ref("dim_customer")}} b
ON a.dim_customer_id = b.dim_customer_id
GROUP BY b.customer_id,b.customer_name )

SELECT 
  customer_id
, customer_name
, total_revenue
, RANK() OVER(ORDER BY total_revenue DESC) rank_by_revenue
, total_quantity
, RANK() OVER(ORDER BY total_quantity DESC) rank_by_quantity
FROM customer_sales