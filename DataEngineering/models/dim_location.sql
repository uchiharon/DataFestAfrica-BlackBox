{{config(materialized='view')}}

-- Load location data
WITH loaction_tb AS (
    SELECT
        *
    FROM
        {{ ref('stg_location') }}
),

irrigation_tb AS (
    SELECT
        *
    FROM
        {{ ref('stg_irrigation') }}
),

final_tb AS (
    SELECT 
        a.* 
    FROM 
        loaction_tb a
    JOIN 
        irrigation_tb b
    ON 
        a.sensor_id = b.sensor_id and a.location_id = b.location_id
)


SELECT
    *
FROM
    final_tb

-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}
