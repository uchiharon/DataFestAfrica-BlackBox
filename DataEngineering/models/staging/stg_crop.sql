{{config(materialized='view')}}

-- Load the crops raw data
WITH crop_data as (
    SELECT * 
    FROM {{source('staging','CROPDATARAW')}}
),

-- Remove rows with null
crop_data_without_null as (
    SELECT *
    FROM crop_data
    WHERE CROP_TYPE IS NOT NULL and CROP_YIELD IS NOT NULL and GROWTH_STAGE IS NOT NULL and PEST_ISSUE IS NOT NULL
), 


-- Trim string columns
crop_data_trimmed as (
    SELECT
        TRIM(TIMESTAMP) AS TIMESTAMP,
        TRIM(CROP_TYPE) AS CROP_TYPE,
        TRIM(CROP_YIELD) AS CROP_YIELD,
        TRIM(GROWTH_STAGE) AS GROWTH_STAGE,
        TRIM(PEST_ISSUE) AS PEST_ISSUE
    FROM
        crop_data_without_null

),


-- remove rows with na
crop_data_without_na as (
    SELECT *
    FROM crop_data_trimmed
    WHERE CROP_TYPE != 'NA' and CROP_YIELD != 'NA' and GROWTH_STAGE != 'NA' and PEST_ISSUE != 'NA'
),

-- Correct wrong spellings
crop_data_with_correct_spelling as (
    SELECT
        TIMESTAMP,
        CASE
            WHEN CROP_TYPE = 'Cron' THEN 'Corn'
            WHEN CROP_TYPE = 'Wheaat' THEN 'Wheat'
            ELSE CROP_TYPE
        END AS CROP_TYPE,
        CROP_YIELD, 
        CASE
            WHEN GROWTH_STAGE = 'Flowerring' THEN 'Flowering'
            WHEN GROWTH_STAGE = 'Vegatative' THEN 'Vegetative' 
            ELSE GROWTH_STAGE
        END AS GROWTH_STAGE, 
        CASE
            WHEN PEST_ISSUE = 'Aphidds' THEN 'Aphids'
            ELSE PEST_ISSUE
        END AS PEST_ISSUE
    FROM
        crop_data_without_na
),

-- Create an index that would be used for connection in the future.
crop_data_data_index_creation AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY CROP_TYPE, GROWTH_STAGE) AS CROP_ID
    FROM
        crop_data_with_correct_spelling
),


-- Format columns data types
crop_data_dtype_format as (
    SELECT
        DATE_TRUNC('minute', TO_TIMESTAMP(TIMESTAMP, 'MM/DD/YYYY HH24:MI')) AS TIMESTAMP,
        CAST(CROP_ID AS VARCHAR(16)) AS CROP_ID,
        CAST(CROP_TYPE AS VARCHAR(16)) AS CROP_TYPE,
        CAST(CROP_YIELD AS NUMERIC(10,2)) AS CROP_YIELD,
        CAST(GROWTH_STAGE AS VARCHAR(16)) AS GROWTH_STAGE,
        CAST(PEST_ISSUE AS VARCHAR(16)) AS PEST_ISSUE
    FROM
        crop_data_data_index_creation
),

-- Carry out daily summary  
final_tb AS (
    SELECT 
        TIMESTAMP, CROP_ID,
        CROP_TYPE, GROWTH_STAGE, PEST_ISSUE, 
        SUM(CROP_YIELD) AS TOTAL_CROP_YIELD 
    FROM 
        crop_data_dtype_format
    GROUP BY 
        TIMESTAMP, CROP_ID, CROP_TYPE, GROWTH_STAGE, PEST_ISSUE
)





-- Query final result
SELECT
    *
FROM
    final_tb

-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}