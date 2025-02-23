{{ config(
    schema='crime_data_stage', 
    materialized='table'
) }}

SELECT SAFE_CAST(row_id AS STRING) as id,
COALESCE(latitude, 37.773972) as latitude,
COALESCE(longitude, -122.431297) as longitude,
incident_datetime as incident_date,
incident_year,
incident_day_of_week as incident_weekday,
COALESCE(incident_category, 'N/A') as crime_type, --CHANGE/ADD TO THE TYPE ID EVENTUALLY
incident_description,

CASE 
    -- Violent Crimes
    WHEN LOWER(incident_category) LIKE '%assault%' 
      OR LOWER(incident_category) LIKE '%battery%' 
      OR LOWER(incident_category) LIKE '%homicide%' 
      OR LOWER(incident_category) LIKE '%murder%' 
      OR LOWER(incident_category) LIKE '%kidnapping%' 
      OR LOWER(incident_category) LIKE '%rape%' 
      OR LOWER(incident_category) LIKE '%fondling%' 
      OR LOWER(incident_category) LIKE '%intimidation%' 
      OR LOWER(incident_category) LIKE '%extortion%' 
      OR LOWER(incident_category) LIKE '%weapons%' 
      OR LOWER(incident_category) LIKE '%trafficking%' 
      OR LOWER(incident_category) LIKE '%stalking%' 
      OR LOWER(incident_category) LIKE '%robbery%' 
    THEN 'V0001'

    -- Nonviolent Crimes
    WHEN LOWER(incident_category) LIKE '%theft%' 
      OR LOWER(incident_category) LIKE '%burglary%' 
      OR LOWER(incident_category) LIKE '%stolen%' 
      OR LOWER(incident_category) LIKE '%shoplifting%' 
      OR LOWER(incident_category) LIKE '%pocket%' 
      OR LOWER(incident_category) LIKE '%embezzlement%' 
      OR LOWER(incident_category) LIKE '%fraud%' 
      OR LOWER(incident_category) LIKE '%counterfeit%' 
      OR LOWER(incident_category) LIKE '%trespass%' 
      OR LOWER(incident_category) LIKE '%arson%' 
      OR LOWER(incident_category) LIKE '%vandalism%' 
      OR LOWER(incident_category) LIKE '%damage%' 
      OR LOWER(incident_category) LIKE '%dui%' 
      OR LOWER(incident_category) LIKE '%drug%' 
      OR LOWER(incident_category) LIKE '%narcotics%' 
      OR LOWER(incident_category) LIKE '%liquor%' 
      OR LOWER(incident_category) LIKE '%prostitution%' 
      OR LOWER(incident_category) LIKE '%sex offense%' 
      OR LOWER(incident_category) LIKE '%gambling%' 
      OR LOWER(incident_category) LIKE '%obscene%' 
      OR LOWER(incident_category) LIKE '%deceptive%' 
      OR LOWER(incident_category) LIKE '%forgery%' 
    THEN 'N0001'

    -- Other / Administrative / Non-Criminal Cases
    WHEN LOWER(incident_category) LIKE '%non-criminal%' 
      OR LOWER(incident_category) LIKE '%missing person%' 
      OR LOWER(incident_category) LIKE '%suicide%' 
      OR LOWER(incident_category) LIKE '%suspicious%' 
      OR LOWER(incident_category) LIKE '%case closure%' 
      OR LOWER(incident_category) LIKE '%miscellaneous%' 
      OR LOWER(incident_category) LIKE '%public peace%' 
      OR LOWER(incident_category) LIKE '%interference%' 
      OR LOWER(incident_category) LIKE '%family%' 
      OR LOWER(incident_category) LIKE '%vehicle%' 
      OR LOWER(incident_category) LIKE '%traffic%' 
      OR LOWER(incident_category) LIKE '%animal%' 
      OR LOWER(incident_category) LIKE '%courtesy%' 
      OR LOWER(incident_category) LIKE '%fire%' 
      OR LOWER(incident_category) LIKE '%other%' 
    THEN 'O0001'

    ELSE 'O0001' -- Catch-all for any uncategorized cases
  END AS crime_category_id

FROM {{ source('crime_data', 'sanfrancisco_crime') }}