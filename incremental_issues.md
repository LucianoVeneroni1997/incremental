{{ config(
    materialized='incremental',
    unique_key='unique_id'
) }}

SELECT unique_id, value, last_updated
FROM {{ source('my_project', 'my_table') }}
{% if is_incremental() %}
WHERE last_updated > (SELECT MAX(last_updated) FROM {{ this }})
{% endif %}
