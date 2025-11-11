{{ config( 
schema='sales_data_dwh',      
materialized='incremental',
unique_key='dim_product_id',
incremental_strategy='merge',
tags=['dwh', 'silver_layer', 'dimension'],
enabled = True)
}}


SELECT DISTINCT

{{ dbt_utils.generate_surrogate_key(['product_name']) }} AS dim_product_id
    
  ,product_name
  ,category
  ,unit_price

-- FROM sales_data_project.sales_data.sales_data_curated
FROM {{ ref("sales_data_curated") }}
ORDER BY product_name