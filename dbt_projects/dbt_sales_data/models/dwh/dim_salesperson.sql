{{ config( 
schema='sales_data_dwh',      
materialized='incremental',
unique_key='dim_salesperson_id',
incremental_strategy='merge',
tags=['dwh', 'silver_layer', 'dimension'],
enabled = True)
}}


SELECT DISTINCT

{{ dbt_utils.generate_surrogate_key(['salesperson','region']) }} AS dim_salesperson_id

,salesperson
,region

-- FROM sales_data_project.sales_data.sales_data_curated
FROM {{ ref("sales_data_curated") }}
ORDER BY 1;