{{ config( 
schema='sales_data_dwh',      
materialized='incremental',
unique_key='order_id',
incremental_strategy='merge',
tags=['dwh', 'silver_layer', 'fact'],
enabled = True)
}}

SELECT
     order_date.dim_date_id AS dim_order_date_id
    ,shipped_date.dim_date_id AS dim_shipped_date_id
    ,customer.dim_customer_id AS dim_customer_id
    ,region.dim_region_id AS dim_region_id
    ,salesperson.dim_salesperson_id AS dim_salesperson_id
    ,product.dim_product_id AS dim_product_id
    ,ship.dim_ship_id AS dim_ship_id
    ,ship_region.dim_region_id AS dim_ship_region_id
    ,order_id
    ,payment_type
    ,quantity
    ,revenue
    ,shipping_fee
    ,revenue_bins

FROM {{ ref("sales_data_curated") }} fact

JOIN {{ref("dim_date")}} order_date
ON fact.order_date = order_date.date

JOIN {{ref("dim_date")}} shipped_date
ON fact.shipped_date = shipped_date.date

JOIN {{ ref("dim_customer")  }} customer
ON fact.customer_id = customer.customer_id

JOIN {{ ref("dim_region") }} region
ON fact.city = region.city
AND fact.state = region.state
AND fact.country_region = region.country_region

JOIN {{ ref("dim_salesperson") }} salesperson
ON fact.salesperson = salesperson.salesperson
AND fact.region = salesperson.region

JOIN {{ ref("dim_product") }} product
ON fact.product_name = product.product_name

JOIN {{ ref("dim_ship") }} ship
ON fact.shipper_name = ship.shipper_name
AND fact.ship_name = ship.ship_name

JOIN {{ ref("dim_region") }} ship_region
ON ship.dim_region_id = ship_region.dim_region_id
AND fact.ship_city = ship_region.city
AND fact.ship_state = ship_region.state
AND fact.ship_country_region = ship_region.country_region