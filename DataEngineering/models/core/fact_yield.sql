{{config(materialized='table')}}

WITH final_tb AS (
    SELECT
        TIMESTAMP, CROP_ID, PEST_ID, TOTAL_CROP_YIELD
    FROM
        {{ ref("int_crop_pest") }}
)

SELECT
    *
FROM
    final_tb

-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}