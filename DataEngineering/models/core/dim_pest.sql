{{config(materialized='table',
    unique_key='PEST_ID')}}

-- Load the staging data for pests
WITH pest_tb AS (
    SELECT 
        PEST_ID, PEST_TYPE, PEST_SEVERITY, PEST_DESCRIPTION
    FROM 
        {{ ref("stg_pest") }}
    GROUP BY 
        PEST_ID, PEST_TYPE, PEST_SEVERITY, PEST_DESCRIPTION
    ORDER BY PEST_ID
)

-- Query final table
SELECT
    *
FROM
    pest_tb


-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}