{{ config( 
schema='sales_data_dwh',      
materialized='incremental',
unique_key='dim_shipper_id',
incremental_strategy='merge',
tags=['dwh', 'silver_layer', 'dimension'],
enabled = True)
}}

SELECT DISTINCT

{{ dbt_utils.generate_surrogate_key(['shipper_name']) }} AS dim_shipper_id
,shipper_name
-- FROM sales_data_project.sales_data.sales_data_curated 
FROM {{ ref("sales_data_curated") }}