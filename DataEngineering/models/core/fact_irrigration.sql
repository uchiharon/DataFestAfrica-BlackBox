{{config(materialized='table')}}


SELECT
    *
FROM
    {{ ("stg_irrigation") }}

-- Create a variable for running test
{% if var('is_test_run', default=true) %}

limit 200

{% endif %}