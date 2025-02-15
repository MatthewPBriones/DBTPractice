SELECT *
FROM {{ source('crime_data', 'chicago_crime') }}