{{ config( 
schema='sales_data_dwh',      
materialized='incremental',
unique_key='dim_date_id',
incremental_strategy='merge',
tags=['dwh', 'silver_layer', 'dimension'],
enabled = True)
}}


WITH order_date AS
( SELECT DISTINCT
 REPLACE( CAST(order_date AS STRING), '-','') AS dim_date_id
,order_date AS date
,MONTH(order_date) AS month
,DATE(date_trunc('MONTH', order_date)) AS mtd_start
,QUARTER(order_date) AS quarter
,DATE(date_trunc('QUARTER', order_date)) AS qtd_start
,YEAR(order_date) AS year
,DATE(date_trunc('YEAR', order_date)) AS ytd_start 
-- FROM sales_data_project.sales_data.sales_data_curated
FROM {{ ref("sales_data_curated") }}
ORDER BY 1 )


, shipped_date AS
( SELECT DISTINCT
 REPLACE( CAST(shipped_date AS STRING), '-','') AS dim_date_id
,shipped_date as date
,MONTH(shipped_date) AS month
,DATE(date_trunc('MONTH', shipped_date)) AS mtd_start
,QUARTER(shipped_date) AS quarter
,DATE(date_trunc('QUARTER', shipped_date)) AS qtd_start
,YEAR(shipped_date) AS year
,DATE(date_trunc('YEAR', shipped_date)) AS ytd_start 
-- FROM sales_data_project.sales_data.sales_data_curated
FROM {{ ref("sales_data_curated") }}
ORDER BY 1 ) 

SELECT * FROM shipped_date
UNION DISTINCT
SELECT * FROM order_date
ORDER BY date