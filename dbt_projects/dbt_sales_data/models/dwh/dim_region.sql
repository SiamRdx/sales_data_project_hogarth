{{ config( 
schema='sales_data_dwh',      
materialized='incremental',
unique_key='dim_region_id',
incremental_strategy='merge',
tags=['dwh', 'silver_layer', 'dimension'],
enabled = True)
}}


WITH region AS (
SELECT DISTINCT
{{ dbt_utils.generate_surrogate_key(['city','state','country_region']) }} AS dim_region_id
,city
,state
,country_region

-- FROM sales_data_project.sales_data.sales_data_curated
FROM {{ ref("sales_data_curated") }}
)

, ship_region AS (
SELECT DISTINCT
{{ dbt_utils.generate_surrogate_key(['ship_city','ship_state','ship_country_region']) }} AS dim_region_id
,ship_city AS city
,ship_state AS state
,ship_country_region AS country_region

-- FROM sales_data_project.sales_data.sales_data_curated
FROM {{ ref("sales_data_curated") }}
)

SELECT * FROM region
UNION DISTINCT
SELECT * FROM ship_region