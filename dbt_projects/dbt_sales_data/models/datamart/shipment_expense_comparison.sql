{{ config( 
schema='sales_data_datamart',      
materialized='table',
tags=['datamart', 'gold_layer', 'analysis'],
enabled = True)
}}



-- f. Shipment expense comparison by shipping company
-- Consideration of average shipment expense per unit for each product across different shipping companies. Shipping company names in columns have been changed to all lowecase with underscores.

with shipping_product_sales AS (
SELECT 
  d.shipper_name
, b.product_name
, e.state as ship_state
, e.city as ship_city  
, SUM(a.shipping_fee) AS total_shipping_fee
, SUM(a.quantity) AS total_quantity

FROM
{{ref("fact_sales")}} a 
JOIN {{ref("dim_product")}} b
ON a.dim_product_id = b.dim_product_id
JOIN {{ref("dim_customer")}} c
ON a.dim_customer_id = c.dim_customer_id
JOIN {{ref("dim_shipper")}} d
ON a.dim_shipper_id = d.dim_shipper_id
JOIN {{ref("dim_region")}} e
ON c.dim_region_id = e.dim_region_id
GROUP BY 1,2,3,4
)

, shipping_fee_per_product AS (

SELECT 
 shipper_name
,product_name
,ship_state
,ship_city
,CASE WHEN total_quantity IS NOT NULL AND total_quantity <> 0 THEN IFNULL(total_shipping_fee,0)/total_quantity
ELSE 0 END as mean_shipping_fee
FROM shipping_product_sales WHERE total_quantity IS NOT NULL AND total_quantity <> 0
)




SELECT *
FROM (
    SELECT 
        product_name,
        ship_state,
        ship_city,
        shipper_name,
        mean_shipping_fee
    FROM shipping_fee_per_product
)
PIVOT (
    AVG(mean_shipping_fee)
    FOR shipper_name IN (
        'Shipping Company A' AS shipping_company_a,
        'Shipping Company B' AS shipping_company_b,
        'Shipping Company C' AS shipping_company_c
    )
)
ORDER BY product_name