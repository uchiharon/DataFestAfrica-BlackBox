{{config(materialized='table',
    unique_key='WEATHER_ID')}}

--  Load weather data and summarise
WITH final_tb AS (
    SELECT 
        WEATHER_ID, WEATHER_CONDITION
    FROM 
        {{ ref(("stg_weather")) }}
    GROUP BY 
        WEATHER_ID, WEATHER_CONDITION
    ORDER BY  
        WEATHER_ID
)

SELECT
    *
FROM
    final_tb


-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}