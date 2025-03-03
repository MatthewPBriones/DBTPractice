{{ config(
    schema='crime_data_stage', 
    materialized='table'
) }}
with final as (
SELECT CONCAT('SEA', SAFE_CAST(report_number AS STRING), SAFE_CAST(offense_id AS STRING)) as id,

case
when COALESCE(latitude, 0) > 0 then latitude
else 47.608013
 end as latitude,

case
when COALESCE(longitude, 0) > 0 then longitude
else -122.335167
 end as longitude,

offense_start_datetime as incident_date,
extract(year from offense_start_datetime) as incident_year,
FORMAT_DATE('%A', DATE(offense_start_datetime)) as incident_weekday,
COALESCE(offense, 'N/A') as crime_type, --CHANGE/ADD TO THE TYPE ID EVENTUALLY
'NO DESCRIPTION AVAILABLE' as incident_description,
'SEATTLE' AS city,

CASE 
    -- Violent Crimes
    WHEN LOWER(offense) LIKE '%assault%' 
      OR LOWER(offense) LIKE '%battery%' 
      OR LOWER(offense) LIKE '%homicide%' 
      OR LOWER(offense) LIKE '%murder%' 
      OR LOWER(offense) LIKE '%kidnapping%' 
      OR LOWER(offense) LIKE '%rape%' 
      OR LOWER(offense) LIKE '%fondling%' 
      OR LOWER(offense) LIKE '%intimidation%' 
      OR LOWER(offense) LIKE '%extortion%' 
      OR LOWER(offense) LIKE '%weapons%' 
      OR LOWER(offense) LIKE '%trafficking%' 
      OR LOWER(offense) LIKE '%stalking%' 
      OR LOWER(offense) LIKE '%robbery%' 
    THEN 'V0001'

    -- Nonviolent Crimes
    WHEN LOWER(offense) LIKE '%theft%' 
      OR LOWER(offense) LIKE '%burglary%' 
      OR LOWER(offense) LIKE '%stolen%' 
      OR LOWER(offense) LIKE '%shoplifting%' 
      OR LOWER(offense) LIKE '%pocket%' 
      OR LOWER(offense) LIKE '%embezzlement%' 
      OR LOWER(offense) LIKE '%fraud%' 
      OR LOWER(offense) LIKE '%counterfeit%' 
      OR LOWER(offense) LIKE '%trespass%' 
      OR LOWER(offense) LIKE '%arson%' 
      OR LOWER(offense) LIKE '%vandalism%' 
      OR LOWER(offense) LIKE '%damage%' 
      OR LOWER(offense) LIKE '%dui%' 
      OR LOWER(offense) LIKE '%drug%' 
      OR LOWER(offense) LIKE '%narcotics%' 
      OR LOWER(offense) LIKE '%liquor%' 
      OR LOWER(offense) LIKE '%prostitution%' 
      OR LOWER(offense) LIKE '%sex offense%' 
      OR LOWER(offense) LIKE '%gambling%' 
      OR LOWER(offense) LIKE '%obscene%' 
      OR LOWER(offense) LIKE '%deceptive%' 
      OR LOWER(offense) LIKE '%forgery%' 
    THEN 'N0001'

    -- Other / Administrative / Non-Criminal Cases
    WHEN LOWER(offense) LIKE '%non-criminal%' 
      OR LOWER(offense) LIKE '%missing person%' 
      OR LOWER(offense) LIKE '%suicide%' 
      OR LOWER(offense) LIKE '%suspicious%' 
      OR LOWER(offense) LIKE '%case closure%' 
      OR LOWER(offense) LIKE '%miscellaneous%' 
      OR LOWER(offense) LIKE '%public peace%' 
      OR LOWER(offense) LIKE '%interference%' 
      OR LOWER(offense) LIKE '%family%' 
      OR LOWER(offense) LIKE '%vehicle%' 
      OR LOWER(offense) LIKE '%traffic%' 
      OR LOWER(offense) LIKE '%animal%' 
      OR LOWER(offense) LIKE '%courtesy%' 
      OR LOWER(offense) LIKE '%fire%' 
      OR LOWER(offense) LIKE '%other%' 
    THEN 'O0001'

    ELSE 'O0001' -- Catch-all for any uncategorized cases
  END AS crime_category_id
FROM {{ source('crime_data', 'seattle_crime') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY report_number, offense_id ORDER BY offense_start_datetime DESC) = 1
)

select *
from final