{{config(materialized='table',
    unique_key='TIMESTAMP')}}

WITH sensor_tb AS (
    SELECT 
        TIMESTAMP
    FROM 
        {{ ref("stg_sensor") }}
    GROUP BY 
        TIMESTAMP
    ORDER BY TIMESTAMP
),

final_tb AS (
    SELECT
        TIMESTAMP,
        DATE_PART(HOUR, TIMESTAMP) AS hour,
        DATE_PART(MINUTE, TIMESTAMP) AS minute,
        DATE_PART(MONTH, TIMESTAMP) AS month,
        DATE_PART(YEAR, TIMESTAMP) AS year,
        DAYOFWEEK(TIMESTAMP) AS day_of_week,
        DATE_PART(WEEK, TIMESTAMP) AS week_of_year
    FROM
        sensor_tb
)

SELECT
    *
FROM
    final_tb


-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}