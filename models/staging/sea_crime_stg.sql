SELECT *
FROM {{ source('crime_data', 'seattle_crime') }}