{{ config(
    schema='crime_data_prod', 
    materialized='table'
) }}

SELECT *
FROM {{ ref('crime_type_stg') }}