{{config(materialized='view')}}

-- Load the staging data of crops
WITH crop_tb AS (
    SELECT 
        *
    FROM 
        {{ ref("stg_crop") }}
),

-- Load the staging data for pests
pest_tb AS (
    SELECT 
        TIMESTAMP, PEST_TYPE, PEST_ID
    FROM 
        {{ ref("stg_pest") }}
),

-- Combine the tables to build an intermediate fact table
final_tb AS (
    SELECT 
        a.TIMESTAMP, a.CROP_ID, a.CROP_TYPE, a.GROWTH_STAGE, a.TOTAL_CROP_YIELD, b.PEST_ID
    FROM 
        crop_tb a
    JOIN 
        pest_tb b
    ON a.timestamp = b.timestamp and a.pest_issue = b.pest_type
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
