with a as (
    select CASE 
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

UNION ALL 

SELECT CASE 
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

UNION ALL 

SELECT CASE 
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
)
select distinct crime_category_id,
CASE
WHEN crime_category_id = 'V0001' THEN 'Violent'
WHEN crime_category_id = 'N0001' THEN 'Nonviolent'
WHEN crime_category_id = 'O0001' THEN 'Other' 
END AS crime_category
from a