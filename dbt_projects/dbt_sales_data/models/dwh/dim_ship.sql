{{ config( 
schema='sales_data_dwh',      
materialized='incremental',
unique_key='dim_ship_id',
incremental_strategy='merge',
tags=['dwh', 'silver_layer', 'dimension'],
enabled = True)
}}



WITH ship_info AS (
SELECT DISTINCT

 shipper_name
,ship_name
,ship_address
,ship_city
,ship_state
,ship_country_region

-- FROM sales_data_project.sales_data.sales_data_curated 
FROM {{ ref("sales_data_curated") }}
)

SELECT

{{ dbt_utils.generate_surrogate_key(['shipper_name','ship_name','ship_city','ship_state','ship_country_region']) }} AS dim_ship_id

,dim_region_id
,shipper_name
,ship_name
,ship_address

FROM ship_info a
JOIN {{ ref("dim_region") }} b
ON  a.ship_city = b.city
AND a.ship_state = b.state
AND a.ship_country_region = b.country_region