{{ config( 
schema='sales_data_dwh',      
materialized='incremental',
unique_key='dim_customer_id',
incremental_strategy='merge',
tags=['dwh', 'silver_layer', 'dimension'],
enabled = True)
}}

WITH customer_info AS (
SELECT DISTINCT
     customer_id
    ,customer_name
    ,ship_name
    ,ship_address
    ,ship_city
    ,ship_state
    ,ship_country_region
-- FROM sales_data_project.sales_data.sales_data_curated
FROM {{ ref("sales_data_curated") }}
)

SELECT
{{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS dim_customer_id

,dim_region_id
,customer_id
,customer_name
,ship_name
,ship_address

FROM customer_info a
JOIN {{ ref("dim_region") }} b
ON  a.ship_city = b.city
AND a.ship_state = b.state
AND a.ship_country_region = b.country_region

ORDER BY customer_id