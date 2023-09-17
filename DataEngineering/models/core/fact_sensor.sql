{{config(materialized='table')}}

--  Load weather data and summarise
WITH weather_tb AS (
    SELECT 
        TIMESTAMP, 
        MIN(WEATHER_ID) AS WEATHER_ID, 
        CAST(AVG(AVG_WIND_SPEED) AS NUMERIC(10,2)) AS AVG_WIND_SPEED, 
        CAST(AVG(AVG_PRECIPITATION) AS NUMERIC(10,2)) AS AVG_PRECIPITATION 
    FROM 
        {{ ref(("stg_weather")) }}
    GROUP BY 
        TIMESTAMP
),

soil_tb AS (
    SELECT 
        TIMESTAMP,
        CAST(AVG_SOIL_COMP AS NUMERIC(10,2)) AS AVG_SOIL_COMP,
        CAST(AVG_SOIL_MOISTURE AS NUMERIC(10,2)) AS AVG_SOIL_MOISTURE,
        CAST(AVG_SOIL_PH AS NUMERIC(10,2)) AS AVG_SOIL_PH,
        CAST(AVG_NITROGEN_LEVEL AS NUMERIC(10,2)) AS AVG_NITROGEN_LEVEL,
        CAST(AVG_PHOSPHORUS_LEVEL AS NUMERIC(10,2)) AS AVG_PHOSPHORUS_LEVEL,
        CAST(AVG_ORGANIC_MATTER AS NUMERIC(10,2)) AS AVG_ORGANIC_MATTER
    FROM 
        {{ ref("stg_soil") }}
),

-- Load sensor data an summarise
sensor_tb AS (
    SELECT 
        TIMESTAMP, 
        CAST(AVG(AVG_TEMPERATURE) AS NUMERIC(10,2)) AS AVG_TEMPERATURE, 
        CAST(AVG(AVG_HUMIDITY) AS NUMERIC(10,2)) AS AVG_HUMIDITY, 
        CAST(AVG(AVG_SOIL_MOISTURE) AS NUMERIC(10,2)) AS AVG_SOIL_MOISTURE, 
        CAST(AVG(AVG_LIGHT_INTENSITY) AS NUMERIC(10,2)) AS AVG_LIGHT_INTENSITY, 
        CAST(AVG(AVG_BATTERY_LEVEL) AS NUMERIC(10,2)) AS AVG_BATTERY_LEVEL
    FROM 
        {{ ref("stg_sensor") }}
    GROUP BY 
        TIMESTAMP
),

final_tb AS (
    SELECT 
        s.*, so.AVG_SOIL_COMP, so.AVG_SOIL_PH, so.AVG_NITROGEN_LEVEL, so.AVG_PHOSPHORUS_LEVEL, 
        so.AVG_ORGANIC_MATTER, w.WEATHER_ID, w.AVG_WIND_SPEED, w.AVG_PRECIPITATION

    FROM 
        sensor_tb s
    JOIN 
        weather_tb w
    ON 
        s.TIMESTAMP = w.TIMESTAMP
    JOIN 
        soil_tb so
    ON 
        s.TIMESTAMP = so.TIMESTAMP
)

SELECT
    *
FROM
    final_tb


-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}