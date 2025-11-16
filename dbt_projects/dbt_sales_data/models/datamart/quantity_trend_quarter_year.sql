{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- e. Revenue, Quantity & Sales trend analysis by Year, Quarter & Month
-- side by side comparison of quantity trend over quarters each year

with monthly_sales AS (
SELECT 
 b.mtd_start as month
-- , SUM(IFNULL(a.revenue,0)) AS revenue
, SUM(IFNULL(a.quantity,0)) AS quantity
 FROM 
{{ ref("fact_sales") }} a  
JOIN {{ ref("dim_date") }} b
ON a.dim_order_date_id = b.dim_date_id
GROUP BY b.mtd_start ) 


SELECT *
FROM (
    SELECT 
     YEAR(month) AS YEAR
    ,QUARTER(month) AS QUARTER
    , quantity
    FROM monthly_sales
)
PIVOT (
    SUM(quantity)
  FOR QUARTER in (
     1 AS Q1
    ,2 AS Q2
    ,3 AS Q3
    ,4 AS Q4
  )
)
ORDER BY YEAR