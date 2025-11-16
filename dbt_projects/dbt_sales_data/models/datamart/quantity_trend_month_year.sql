{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- e. Revenue, Quantity & Sales trend analysis by Year, Quarter & Month
-- side by side comparison of quantity trend over months each year

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
    ,MONTH(month) AS MONTH
    , quantity
    FROM monthly_sales
)
PIVOT (
    SUM(quantity)
  FOR MONTH in (
    1 JAN, 2 FEB, 3 MAR, 4 APR, 5 MAY, 6 JUN,
    7 JUL, 8 AUG, 9 SEP, 10 OCT, 11 NOV, 12 DEC
  )
)
ORDER BY YEAR