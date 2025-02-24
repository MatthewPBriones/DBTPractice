{{ config(
    schema='crime_data_prod', 
    materialized='table'
) }}

select *
from {{ ref('chi_crime_stg') }}

UNION ALL

select *

from {{ ref('sf_crime_stg') }}

UNION ALL 

select *
from {{ ref("sea_crime_stg") }}
