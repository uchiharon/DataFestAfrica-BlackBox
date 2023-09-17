{{config(materialized='view')}}

-- Load the sensor raw data
WITH sensor_data as (
    SELECT * 
    FROM {{source('staging','SENSORDATARAW')}}
),

-- Remove all null values
sensor_data_without_null AS (
    SELECT
        *
    FROM
        sensor_data
    WHERE
        TEMPERATURE IS NOT NULL and HUMIDITY IS NOT NULL and SOIL_MOISTURE IS NOT NULL and 
        LIGHT_INTENSITY IS NOT NULL and BATTERY_LEVEL IS NOT NULL
),

-- Remove all NA values
sensor_data_without_na AS (
    SELECT
        *
    FROM
        sensor_data_without_null
    WHERE
        TEMPERATURE != 'NA' and HUMIDITY != 'NA' and SOIL_MOISTURE != 'NA' and 
        LIGHT_INTENSITY != 'NA' and BATTERY_LEVEL != 'NA'
),

-- formal the string columns 
sensor_data_str_format AS (
    SELECT
        TRIM(REPLACE(SENSOR_ID, '"', '')) AS SENSOR_ID, 
        TRIM(REPLACE(TIMESTAMP, '"', '')) AS TIMESTAMP,
        TRIM(TEMPERATURE) AS TEMPERATURE, 
        TRIM(HUMIDITY) AS HUMIDITY,
        TRIM(SOIL_MOISTURE) AS SOIL_MOISTURE, 
        TRIM(LIGHT_INTENSITY) AS LIGHT_INTENSITY, 
        TRIM(BATTERY_LEVEL) AS BATTERY_LEVEL
    FROM
        sensor_data_without_na
),


-- format field data types
final_tb AS(
    SELECT
        
        CAST(SENSOR_ID AS VARCHAR(8)) AS SENSOR_ID,
        DATE_TRUNC('minute', TO_TIMESTAMP(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')) AS TIMESTAMP,
        CAST(TEMPERATURE AS NUMERIC(5,1)) AS TEMPERATURE,
        CAST(HUMIDITY AS NUMERIC(5,1)) AS HUMIDITY,
        CAST(SOIL_MOISTURE AS NUMERIC(5,1)) AS SOIL_MOISTURE,
        CAST(LIGHT_INTENSITY AS NUMERIC(5,0)) AS LIGHT_INTENSITY,
        CAST(BATTERY_LEVEL AS NUMERIC(5,1)) AS BATTERY_LEVEL
        
    FROM
        sensor_data_str_format
)

-- query final table
SELECT
    *
FROM
    final_tb

-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}