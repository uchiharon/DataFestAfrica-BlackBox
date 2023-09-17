{{config(materialized='view')}}

-- Load the pests raw data
WITH pest_data as (
    SELECT * 
    FROM {{source('staging','PESTDATARAW')}}
),

-- Remove all null values
pest_data_without_null AS (
    SELECT
        *
    FROM
        pest_data
    WHERE
        PEST_TYPE IS NOT NULL and PEST_DESCRIPTION IS NOT NULL and PEST_SEVERITY IS NOT NULL
),

-- Remove all NA values
pest_data_without_na AS (
    SELECT
        *
    FROM
        pest_data_without_null
    WHERE
        PEST_TYPE != 'NA' and PEST_DESCRIPTION != 'NA' and PEST_SEVERITY != 'NA'
),

-- Split the PEST_DESCRIPTION Column and extract just the description
pest_data_des_cleaning AS (
    SELECT
        TIMESTAMP,
        PEST_TYPE,
        INITCAP(SPLIT_PART(SPLIT_PART(PEST_DESCRIPTION, ':', 2), '.', 1)) AS PEST_DESCRIPTION,
        PEST_SEVERITY
    FROM
        pest_data_without_na
    
),

-- Trim the strings white spaces
pest_data_trimmed AS (
    SELECT
        TRIM(TIMESTAMP) AS TIMESTAMP,
        TRIM(PEST_TYPE) AS PEST_TYPE,
        TRIM(PEST_DESCRIPTION) AS PEST_DESCRIPTION,
        TRIM(PEST_SEVERITY) AS PEST_SEVERITY
    FROM
        pest_data_des_cleaning
),

-- Correct wrong spellings
pest_data_spelling_correction AS (
    SELECT 
        TIMESTAMP,
        CASE
            WHEN PEST_TYPE = 'Aphiods' THEN 'Aphids'
            WHEN PEST_TYPE = 'Slogs' THEN 'Slugs'
            ELSE PEST_TYPE
        END AS PEST_TYPE,
        CASE
            WHEN PEST_DESCRIPTION = 'High Infestation Riskkk' THEN 'High Infestation Risk'
            WHEN PEST_DESCRIPTION = 'Infestation Deteced' THEN 'Infestation Detected'
            ELSE PEST_DESCRIPTION
        END AS PEST_DESCRIPTION,
        CASE
            WHEN PEST_SEVERITY = 'Hihg' THEN 'High'
            ELSE PEST_SEVERITY
        END AS PEST_SEVERITY
    FROM 
        pest_data_trimmed
),

-- Create an index that would be used for connection in the future.
pest_data_index_creation AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY PEST_TYPE, PEST_DESCRIPTION, PEST_SEVERITY) AS PEST_ID
    FROM
        pest_data_spelling_correction
),

-- Select data types of each column
pest_data_dtype_format AS (
    SELECT
        DATE_TRUNC('minute', TO_TIMESTAMP(TIMESTAMP, 'MM/DD/YYYY HH24:MI')) AS TIMESTAMP,
        CAST(PEST_ID AS INT) AS PEST_ID,
        CAST(PEST_TYPE AS VARCHAR(16)) AS PEST_TYPE,
        CAST(PEST_DESCRIPTION AS VARCHAR(32)) AS PEST_DESCRIPTION,
        CAST(PEST_SEVERITY AS VARCHAR(8)) AS PEST_SEVERITY
    FROM
        pest_data_index_creation
),

-- Get daily summary
final_tb AS (
    SELECT 
        TIMESTAMP, 
        PEST_DESCRIPTION, PEST_SEVERITY, PEST_TYPE, PEST_ID
    FROM 
        pest_data_dtype_format
    GROUP BY 
        TIMESTAMP, PEST_DESCRIPTION, PEST_SEVERITY, PEST_TYPE, PEST_ID 
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