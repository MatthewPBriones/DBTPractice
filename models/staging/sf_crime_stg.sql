SELECT *
FROM {{ source('crime_data', 'sanfrancisco_crime') }}