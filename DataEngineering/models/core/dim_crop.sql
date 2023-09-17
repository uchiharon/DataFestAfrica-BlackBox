{{config(materialized='table',
    unique_key='CROP_ID')}}

WITH final_tb AS (
    SELECT
        CROP_ID, CROP_TYPE, GROWTH_STAGE
    FROM
        {{ ref("stg_crop") }}
    GROUP BY
        CROP_ID, CROP_TYPE, GROWTH_STAGE
    ORDER BY CROP_ID
)

SELECT
    *
FROM
    final_tb

-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}