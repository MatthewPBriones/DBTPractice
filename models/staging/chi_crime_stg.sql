{{ config(
    materialized='table', 
    version=2               
) }}

SELECT id,
COALESCE(latitude, 41.881832) as latitude,
COALESCE(longitude, -87.623177) as longitude,
date as incident_date,
year as incident_year,
FORMAT_DATE('%A', DATE(date)) as incident_weekday,
COALESCE(primary_type, 'N/A') as crime_type, --CHANGE/ADD TO THE TYPE ID EVENTUALLY
description as incident_description,

CASE 
    -- Violent Crimes
    WHEN LOWER(primary_type) LIKE '%assault%' 
      OR LOWER(primary_type) LIKE '%battery%' 
      OR LOWER(primary_type) LIKE '%homicide%' 
      OR LOWER(primary_type) LIKE '%murder%' 
      OR LOWER(primary_type) LIKE '%kidnapping%' 
      OR LOWER(primary_type) LIKE '%rape%' 
      OR LOWER(primary_type) LIKE '%fondling%' 
      OR LOWER(primary_type) LIKE '%intimidation%' 
      OR LOWER(primary_type) LIKE '%extortion%' 
      OR LOWER(primary_type) LIKE '%weapons%' 
      OR LOWER(primary_type) LIKE '%trafficking%' 
      OR LOWER(primary_type) LIKE '%stalking%' 
      OR LOWER(primary_type) LIKE '%robbery%' 
    THEN 'V0001'

    -- Nonviolent Crimes
    WHEN LOWER(primary_type) LIKE '%theft%' 
      OR LOWER(primary_type) LIKE '%burglary%' 
      OR LOWER(primary_type) LIKE '%stolen%' 
      OR LOWER(primary_type) LIKE '%shoplifting%' 
      OR LOWER(primary_type) LIKE '%pocket%' 
      OR LOWER(primary_type) LIKE '%embezzlement%' 
      OR LOWER(primary_type) LIKE '%fraud%' 
      OR LOWER(primary_type) LIKE '%counterfeit%' 
      OR LOWER(primary_type) LIKE '%trespass%' 
      OR LOWER(primary_type) LIKE '%arson%' 
      OR LOWER(primary_type) LIKE '%vandalism%' 
      OR LOWER(primary_type) LIKE '%damage%' 
      OR LOWER(primary_type) LIKE '%dui%' 
      OR LOWER(primary_type) LIKE '%drug%' 
      OR LOWER(primary_type) LIKE '%narcotics%' 
      OR LOWER(primary_type) LIKE '%liquor%' 
      OR LOWER(primary_type) LIKE '%prostitution%' 
      OR LOWER(primary_type) LIKE '%sex offense%' 
      OR LOWER(primary_type) LIKE '%gambling%' 
      OR LOWER(primary_type) LIKE '%obscene%' 
      OR LOWER(primary_type) LIKE '%deceptive%' 
      OR LOWER(primary_type) LIKE '%forgery%' 
    THEN 'N0001'

    -- Other / Administrative / Non-Criminal Cases
    WHEN LOWER(primary_type) LIKE '%non-criminal%' 
      OR LOWER(primary_type) LIKE '%missing person%' 
      OR LOWER(primary_type) LIKE '%suicide%' 
      OR LOWER(primary_type) LIKE '%suspicious%' 
      OR LOWER(primary_type) LIKE '%case closure%' 
      OR LOWER(primary_type) LIKE '%miscellaneous%' 
      OR LOWER(primary_type) LIKE '%public peace%' 
      OR LOWER(primary_type) LIKE '%interference%' 
      OR LOWER(primary_type) LIKE '%family%' 
      OR LOWER(primary_type) LIKE '%vehicle%' 
      OR LOWER(primary_type) LIKE '%traffic%' 
      OR LOWER(primary_type) LIKE '%animal%' 
      OR LOWER(primary_type) LIKE '%courtesy%' 
      OR LOWER(primary_type) LIKE '%fire%' 
      OR LOWER(primary_type) LIKE '%other%' 
    THEN 'O0001'

    ELSE 'O0001' -- Catch-all for any uncategorized cases
  END AS crime_category_id
FROM {{ source('crime_data', 'chicago_crime') }}