{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}


--c. Salesperson ranking by revenue, quantity & Sales

WITH salesperson_sales AS (
SELECT 
  b.salesperson
, SUM(a.revenue) AS total_revenue
, SUM(a.quantity) AS total_quantity

FROM
{{ref("fact_sales")}}  a 
JOIN {{ref("dim_salesperson")}} b
ON a.dim_salesperson_id = b.dim_salesperson_id
GROUP BY 1)

SELECT 

  salesperson
, total_revenue
, RANK() OVER(ORDER BY total_revenue DESC) rank_by_revenue
, total_quantity
, RANK() OVER(ORDER BY total_quantity DESC) rank_by_quantity
FROM salesperson_sales