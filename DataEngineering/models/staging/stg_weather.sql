{{config(materialized='view')}}

-- Load the weather raw data
WITH weather_data as (
    SELECT * 
    FROM {{source('staging','WEATHERDATARAW')}}
),

-- Remove all null values
weather_data_without_null AS (
    SELECT
        *
    FROM
        weather_data
    WHERE
        WEATHER_CONDITION IS NOT NULL and WIND_SPEED IS NOT NULL and PRECIPITATION IS NOT NULL
),

-- Remove all NA values
weather_data_without_na AS (
    SELECT
        *
    FROM
        weather_data_without_null
    WHERE
        WEATHER_CONDITION != 'NA' and WIND_SPEED != 'NA' and PRECIPITATION != 'NA'
),

-- Trim lead and preciding white spaces
weather_data_str_format AS (
    SELECT
        TRIM(TIMESTAMP) AS TIMESTAMP, 
        TRIM(WEATHER_CONDITION) AS WEATHER_CONDITION,
        TRIM(WIND_SPEED) AS WIND_SPEED,
        TRIM(PRECIPITATION) AS PRECIPITATION
    FROM
        weather_data_without_na
),

-- Correct all wrong spellings
weather_data_spelling_correction AS (
SELECT
    TIMESTAMP,
    CASE
        WHEN WEATHER_CONDITION = 'Party Cloudy' THEN 'Partly Cloudy'
        WHEN WEATHER_CONDITION = 'Claar' THEN 'Clear'
        ELSE WEATHER_CONDITION
    END AS WEATHER_CONDITION,
    WIND_SPEED,
    PRECIPITATION
FROM
    weather_data_str_format
),

-- format the table dtype
weather_data_dtype_format AS (
    SELECT
        DATE_TRUNC('minute', TO_TIMESTAMP(TIMESTAMP, 'MM/DD/YYYY HH24:MI')) AS TIMESTAMP,
        CAST(WEATHER_CONDITION AS VARCHAR(16)) AS WEATHER_CONDITION,
        CAST(WIND_SPEED AS NUMERIC(5,1)) AS WIND_SPEED,
        CAST(PRECIPITATION AS NUMERIC(10,2)) AS PRECIPITATION        
    FROM
        weather_data_spelling_correction
),

-- Convert to Daily equivalent
final_tb AS (
    SELECT 
        DATE(TIMESTAMP), WEATHER_CONDITION, AVG(WIND_SPEED) AS AVG_WIND_SPEED, 
        AVG(PRECIPITATION) AS AVG_PRECIPITATION
    FROM 
        weather_data_dtype_format
    GROUP BY 
        DATE(TIMESTAMP), WEATHER_CONDITION
)

-- query final result
SELECT
    *
FROM
    final_tb


-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}
