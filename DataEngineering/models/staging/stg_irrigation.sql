{{config(materialized='view')}}

-- Load the irrigation raw data
WITH irrigation_data as (
    SELECT * 
    FROM {{source('staging','IRRIGATIONDATARAW')}}
),


-- Remove all null values
irrigation_data_without_null AS (
    SELECT
        *
    FROM
        irrigation_data
    WHERE
        IRRIGATION_METHOD IS NOT NULL and WATER_SOURCE IS NOT NULL and IRRIGATION_DURATION_MIN IS NOT NULL
),

-- Remove all NA values
irrigation_data_without_na AS (
    SELECT
        *
    FROM
        irrigation_data_without_null
    WHERE
        IRRIGATION_METHOD != 'NA' and WATER_SOURCE != 'NA' and IRRIGATION_DURATION_MIN != 'NA'
),

-- formal the string columns
irrigation_data_str_format AS (
    SELECT
        TRIM(SPLIT_PART(SPLIT_PART(REPLACE(SENSOR_ID,'""',''), '_##', 1), '_', 2)) AS SENSOR_ID, 
        TRIM(REPLACE(SPLIT_PART(REPLACE(SENSOR_ID,'""',''), '_##', 2),'**','')) AS LOCATION_ID, 
        TRIM(TIMESTAMP) AS TIMESTAMP, 
        TRIM(IRRIGATION_METHOD) AS IRRIGATION_METHOD, 
        TRIM(WATER_SOURCE) AS WATER_SOURCE, 
        TRIM(IRRIGATION_DURATION_MIN) AS IRRIGATION_DURATION_MIN
    FROM
        irrigation_data_without_na
),

-- Fix wrong spellings
irrigation_data_spelling_correction AS(
    SELECT
        SENSOR_ID,
        LOCATION_ID,
        TIMESTAMP,
        CASE
            WHEN IRRIGATION_METHOD = 'Spinkler' THEN 'Sprinkler'
            WHEN IRRIGATION_METHOD = 'Driip' THEN 'Drip'
            ELSE IRRIGATION_METHOD
        END AS IRRIGATION_METHOD,
        CASE
            WHEN WATER_SOURCE = 'Rivver' THEN 'River'
            WHEN WATER_SOURCE = 'Wel' THEN 'Well'
            ELSE WATER_SOURCE
        END AS WATER_SOURCE,
        IRRIGATION_DURATION_MIN
    FROM
        irrigation_data_str_format
),

-- Create an index that would be used for connection in the future.
irrigation_data_index_creation AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY IRRIGATION_METHOD, WATER_SOURCE) AS IRRIGATION_ID
    FROM
        irrigation_data_spelling_correction
),

-- Format each column dtype
irrigation_data_dtype_format AS (
    SELECT
        CAST(SENSOR_ID AS VARCHAR(8)) AS SENSOR_ID,
        CAST(LOCATION_ID AS VARCHAR(8)) AS LOCATION_ID,
        DATE_TRUNC('minute', TO_TIMESTAMP(TIMESTAMP, 'MM/DD/YYYY HH24:MI')) AS TIMESTAMP,
        CAST(IRRIGATION_ID AS VARCHAR(4)) AS IRRIGATION_ID,
        CAST(IRRIGATION_METHOD AS VARCHAR(16)) AS IRRIGATION_METHOD,
        CAST(WATER_SOURCE AS VARCHAR(8)) AS WATER_SOURCE,
        CAST(IRRIGATION_DURATION_MIN AS NUMERIC(5,0)) AS IRRIGATION_DURATION_MIN
    FROM
        irrigation_data_index_creation
),

-- Convert to daily measurement
final_tb AS (
    SELECT 
        TIMESTAMP, SENSOR_ID, LOCATION_ID, IRRIGATION_ID, IRRIGATION_METHOD, WATER_SOURCE,
        SUM(IRRIGATION_DURATION_MIN) AS TOTAL_IRRIGATION_DURATION_MIN
    FROM 
        irrigation_data_dtype_format
    GROUP BY 
        TIMESTAMP, SENSOR_ID, LOCATION_ID, IRRIGATION_ID, IRRIGATION_METHOD, WATER_SOURCE
)

-- Query final table
SELECT
    *
FROM
    final_tb



-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}