{{config(materialized='view')}}

-- Load the soil raw data
WITH soil_data as (
    SELECT * 
    FROM {{source('staging','SOILDATARAW')}}
),

-- Remove all null values
sensor_data_without_null AS (
    SELECT
        *
    FROM
        soil_data
    WHERE
        SOIL_COMP IS NOT NULL and SOIL_MOISTURE IS NOT NULL and SOIL_PH IS NOT NULL and 
        NITROGEN_LEVEL IS NOT NULL and PHOSPHORUS_LEVEL IS NOT NULL and ORGANIC_MATTER IS NOT NULL
),

-- Remove all NA 
sensor_data_without_na AS (
    SELECT
        *
    FROM
        sensor_data_without_null
    WHERE
        SOIL_COMP != 'NA' and SOIL_MOISTURE != 'NA' and SOIL_PH != 'NA' and 
        NITROGEN_LEVEL != 'NA' and PHOSPHORUS_LEVEL != 'NA' and ORGANIC_MATTER != 'NA'
),

-- trim leading and preciding white spaces
sensor_data_trimmed AS (
    SELECT
        TRIM(TIMESTAMP) AS TIMESTAMP,
        TRIM(SOIL_COMP) AS SOIL_COMP,
        TRIM(SOIL_MOISTURE) AS SOIL_MOISTURE,
        TRIM(SOIL_PH) AS SOIL_PH,
        TRIM(NITROGEN_LEVEL) AS NITROGEN_LEVEL,
        TRIM(PHOSPHORUS_LEVEL) AS PHOSPHORUS_LEVEL,
        TRIM(ORGANIC_MATTER) AS ORGANIC_MATTER
    FROM
        sensor_data_without_na
),


-- Format the dtype of each column
final_tb AS (
    SELECT
        DATE_TRUNC('minute', TO_TIMESTAMP(TIMESTAMP, 'MM/DD/YYYY HH24:MI')) AS TIMESTAMP,
        CAST(SOIL_COMP AS NUMERIC(10,2)) AS SOIL_COMP,
        CAST(SOIL_MOISTURE AS NUMERIC(5,1)) AS SOIL_MOISTURE,
        CAST(SOIL_PH AS NUMERIC(10,2)) AS SOIL_PH,
        CAST(NITROGEN_LEVEL AS NUMERIC(10,2)) AS NITROGEN_LEVEL,
        CAST(PHOSPHORUS_LEVEL AS NUMERIC(10,2)) AS PHOSPHORUS_LEVEL,
        CAST(ORGANIC_MATTER AS NUMERIC(10,2)) AS ORGANIC_MATTER
    FROM
        sensor_data_trimmed
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