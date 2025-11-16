{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}

-- g. YTD, MTD, QTD analysis for Revenue, Quantity & Shipment & Fees Growth
-- Consideration: Growth is considered for each sales day (order date) on an ascending basis

WITH daily_sales AS (
SELECT 
  b.date as sales_date
 ,b.mtd_start as mtd_start
 ,b.qtd_start as qtd_start
 ,b.ytd_start as ytd_start
,SUM(a.revenue) AS revenue
,SUM(a.quantity) AS quantity
,SUM(a.shipping_fee) AS shipping_fee

 FROM 
{{ref("fact_sales")}} a  
JOIN {{ref("dim_date")}} b
ON a.dim_order_date_id = b.dim_date_id
GROUP BY b.date,b.mtd_start,b.qtd_start,b.ytd_start
ORDER BY b.date
)

-- SELECT * FROM daily_sales

, daily_sales_cumulative AS (
SELECT
sales_date
,revenue
,SUM(revenue) OVER (PARTITION BY mtd_start ORDER BY sales_date) AS mtd_revenue
,SUM(revenue) OVER (PARTITION BY qtd_start ORDER BY sales_date) AS qtd_revenue
,SUM(revenue) OVER (PARTITION BY ytd_start ORDER BY sales_date) AS ytd_revenue
,quantity
,SUM(quantity) OVER (PARTITION BY mtd_start ORDER BY sales_date) AS mtd_quantity
,SUM(quantity) OVER (PARTITION BY qtd_start ORDER BY sales_date) AS qtd_quantity
,SUM(quantity) OVER (PARTITION BY ytd_start ORDER BY sales_date) AS ytd_quantity
,shipping_fee
,SUM(shipping_fee) OVER (PARTITION BY mtd_start ORDER BY sales_date) AS mtd_shipping_fee
,SUM(shipping_fee) OVER (PARTITION BY qtd_start ORDER BY sales_date) AS qtd_shipping_fee
,SUM(shipping_fee) OVER (PARTITION BY ytd_start ORDER BY sales_date) AS ytd_shipping_fee

FROM daily_sales
)

-- SELECT * FROM daily_sales_cumulative

, daily_sales_previous AS (

  SELECT

  sales_date
  ,revenue
  ,mtd_revenue
  ,LAG(mtd_revenue) OVER (ORDER BY sales_date) AS mtd_revenue_prev
  ,qtd_revenue
  ,LAG(qtd_revenue) OVER (ORDER BY sales_date) AS qtd_revenue_prev
  ,ytd_revenue
  ,LAG(ytd_revenue) OVER (ORDER BY sales_date) AS ytd_revenue_prev
  ,quantity
  ,mtd_quantity
  ,LAG(mtd_quantity) OVER (ORDER BY sales_date) AS mtd_quantity_prev
  ,qtd_quantity
  ,LAG(qtd_quantity) OVER (ORDER BY sales_date) AS qtd_quantity_prev
  ,ytd_quantity
  ,LAG(ytd_quantity) OVER (ORDER BY sales_date) AS ytd_quantity_prev
  ,shipping_fee
  ,mtd_shipping_fee
  ,LAG(mtd_shipping_fee) OVER (ORDER BY sales_date) AS mtd_shipping_fee_prev
  ,qtd_shipping_fee
  ,LAG(qtd_shipping_fee) OVER (ORDER BY sales_date) AS qtd_shipping_fee_prev
  ,ytd_shipping_fee
  ,LAG(ytd_shipping_fee) OVER (ORDER BY sales_date) AS ytd_shipping_fee_prev

  FROM daily_sales_cumulative
)

-- SELECT * FROM daily_sales_previous

, daily_sales_growth AS (
  SELECT
    sales_date

  , mtd_revenue as current_mtd_revenue
  , CASE WHEN mtd_revenue_prev IS NULL THEN 0
          WHEN mtd_revenue_prev = 0 THEN 0
          ELSE ( (mtd_revenue - mtd_revenue_prev)/mtd_revenue_prev )*100
          END AS mtd_revenue_growth 
  , qtd_revenue as current_qtd_revenue
  , CASE WHEN qtd_revenue_prev IS NULL THEN 0
          WHEN qtd_revenue_prev = 0 THEN 0
          ELSE ( (qtd_revenue - qtd_revenue_prev)/qtd_revenue_prev )*100
          END AS qtd_revenue_growth 
  , ytd_revenue as current_ytd_revenue
  , CASE WHEN ytd_revenue_prev IS NULL THEN 0
          WHEN ytd_revenue_prev = 0 THEN 0
          ELSE ( (ytd_revenue - ytd_revenue_prev)/ytd_revenue_prev )*100
          END AS ytd_revenue_growth       


  , mtd_quantity as current_mtd_quantity
  , CASE WHEN mtd_quantity_prev IS NULL THEN 0
          WHEN mtd_quantity_prev = 0 THEN 0
          ELSE ( (mtd_quantity - mtd_quantity_prev)/mtd_quantity_prev )*100
          END AS mtd_quantity_growth 
  , qtd_quantity as current_qtd_quantity
  , CASE WHEN qtd_quantity_prev IS NULL THEN 0
          WHEN qtd_quantity_prev = 0 THEN 0
          ELSE ( (qtd_quantity - qtd_quantity_prev)/qtd_quantity_prev )*100
          END AS qtd_quantity_growth 
  , ytd_quantity as current_ytd_quantity
  , CASE WHEN ytd_quantity_prev IS NULL THEN 0
          WHEN ytd_quantity_prev = 0 THEN 0
          ELSE ( (ytd_quantity - ytd_quantity_prev)/ytd_quantity_prev )*100
          END AS ytd_quantity_growth 


  , mtd_shipping_fee as current_mtd_shipping_fee
  , CASE WHEN mtd_shipping_fee_prev IS NULL THEN 0
          WHEN mtd_shipping_fee_prev = 0 THEN 0
          ELSE ( (mtd_shipping_fee - mtd_shipping_fee_prev)/mtd_shipping_fee_prev )*100
          END AS mtd_shipping_fee_growth 
  , qtd_shipping_fee as current_qtd_shipping_fee
  , CASE WHEN qtd_shipping_fee_prev IS NULL THEN 0
          WHEN qtd_shipping_fee_prev = 0 THEN 0
          ELSE ( (qtd_shipping_fee - qtd_shipping_fee_prev)/qtd_shipping_fee_prev )*100
          END AS qtd_shipping_fee_growth 
  , ytd_shipping_fee as current_ytd_shipping_fee
  , CASE WHEN ytd_shipping_fee_prev IS NULL THEN 0
          WHEN ytd_shipping_fee_prev = 0 THEN 0
          ELSE ( (ytd_shipping_fee - ytd_shipping_fee_prev)/ytd_shipping_fee_prev )*100
          END AS ytd_shipping_fee_growth           

FROM daily_sales_previous
)


SELECT * FROM daily_sales_growth