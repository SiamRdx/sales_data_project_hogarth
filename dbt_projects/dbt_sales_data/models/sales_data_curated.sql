{{ config( 
schema='sales_data',      
materialized='table',
tags=['raw_data', 'curated'],
enabled = True)
}}

WITH sales_data_raw_cte AS (

SELECT

     CAST(order_id AS STRING) AS order_id
    ,CAST(order_date AS DATE) AS order_date
    ,CAST(customer_id AS STRING) AS customer_id
    ,CAST(customer_name AS STRING) AS customer_name
    ,CAST(city AS STRING) AS city
    ,CAST(state AS STRING) AS state
    ,CAST(country_region AS STRING) AS country_region
    ,CAST(salesperson AS STRING) AS salesperson
    ,CAST(region AS STRING) AS region
    ,CAST(shipped_date AS DATE) AS shipped_date
    ,CAST(shipper_name AS STRING) AS shipper_name
    ,CAST(ship_name AS STRING) AS ship_name
    ,CAST(ship_address AS STRING) AS ship_address
    ,CAST(ship_city AS STRING) AS ship_city
    ,CAST(ship_state AS STRING) AS ship_state
    ,CAST(ship_country_region AS STRING) AS ship_country_region
    ,CAST(payment_type AS STRING) AS payment_type
    ,CAST(product_name AS STRING) AS product_name
    ,CAST(category AS STRING) AS category
    ,CAST(unit_price AS DOUBLE) AS unit_price
    ,CAST(quantity AS INT) AS quantity
    ,CAST(revenue AS DOUBLE) AS revenue
    ,CAST(shipping_fee AS DOUBLE) AS shipping_fee
    ,CAST(revenue_bins AS DOUBLE) AS revenue_bins

    FROM sales_data_project.sales_data.sales_data_raw
-- WHERE order_id IS NOT NULL
--   AND order_date IS NOT NULL
--   AND customer_id IS NOT NULL
--   AND product_name IS NOT NULL
--   AND revenue IS NOT NULL
--   AND unit_price IS NOT NULL
--   AND quantity IS NOT NULL
)

, product_price AS (

SELECT product_name, MAX(IFNULL(unit_price,0)) AS unit_price_max
FROM sales_data_raw_cte
GROUP BY product_name 
)

SELECT 
     a.order_id
    ,a.order_date
    ,a.customer_id
    ,a.customer_name
    ,a.city
    ,a.state
    ,a.country_region
    ,a.salesperson
    ,a.region
    ,a.shipped_date
    ,a.shipper_name
    ,a.ship_name
    ,a.ship_address
    ,a.ship_city
    ,a.ship_state
    ,a.ship_country_region
    ,a.payment_type
    ,a.product_name
    ,a.category
    -- ,a.unit_price
    ,COALESCE(a.unit_price,b.unit_price_max) AS unit_price
    -- ,a.quantity
    ,COALESCE(a.quantity,0) AS quantity
    -- ,a.revenue
    ,COALESCE(a.revenue, (COALESCE(a.unit_price,b.unit_price_max)*COALESCE(a.quantity,0)) ) AS revenue
    -- ,a.shipping_fee
    ,IFNULL(a.shipping_fee,0) AS shipping_fee
    ,a.revenue_bins

FROM sales_data_raw_cte a
JOIN product_price b
ON a.product_name = b.product_name