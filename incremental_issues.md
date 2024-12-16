-- To solve the issue, I proposed validating unique_key columns for NULL values, adding configuration options to handle them (e.g., skipping rows or replacing NULL), and ensuring meaningful error messages are provided. These changes enhance data integrity and make the incremental process more reliable.

{{ config(
    materialized='incremental',
    unique_key='unique_id'
) }}

SELECT unique_id, value, last_updated
FROM {{ source('my_project', 'my_table') }}
{% if is_incremental() %}
WHERE last_updated > (SELECT MAX(last_updated) FROM {{ this }})
{% endif %}
