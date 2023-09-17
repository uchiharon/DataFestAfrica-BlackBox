{{config(materialized='view')}}

-- Load the location raw data
WITH location_data as (
    SELECT * 
    FROM {{source('staging','LOCATIONDATARAW')}}
),


-- Remove all null values
location_data_without_null AS (
    SELECT
        *
    FROM
        location_data
    WHERE
        LATITUDE IS NOT NULL and LONGITUDE IS NOT NULL and ELEVATION IS NOT NULL and REGION IS NOT NULL
),

-- Remove all NA values
location_data_without_na AS (
    SELECT
        *
    FROM
        location_data_without_null
    WHERE
        LATITUDE != 'NA' and LONGITUDE != 'NA' and ELEVATION != 'NA' and REGION != 'NA'
),

location_data_str_format AS (
    SELECT
        TRIM(SPLIT_PART(SPLIT_PART(REPLACE(SENSOR_ID,'""',''), '_##', 1), '_', 2)) AS SENSOR_ID, 
        TRIM(LOCATION_NAME) AS LOCATION_NAME, 
        TRIM(LATITUDE) AS LATITUDE, 
        TRIM(LONGITUDE) AS LONGITUDE, 
        TRIM(ELEVATION) AS ELEVATION,
        TRIM(REGION) AS REGION
    FROM
        location_data_without_na

),

final_tb AS (
    SELECT
        CAST(SENSOR_ID AS VARCHAR(8)) AS SENSOR_ID,
        CAST(LOCATION_NAME AS VARCHAR(16)) AS LOCATION_NAME,
        CAST(LATITUDE AS NUMERIC(15,9)) AS LATITUDE,
        CAST(LONGITUDE AS NUMERIC(15,9)) AS LONGITUDE,
        CAST(ELEVATION AS NUMERIC(8,2)) AS ELEVATION,
        CAST(REGION AS VARCHAR(16)) AS REGION
    FROM
        location_data_str_format
)

--  Query final table
SELECT
    *
FROM
    final_tb


    
-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}