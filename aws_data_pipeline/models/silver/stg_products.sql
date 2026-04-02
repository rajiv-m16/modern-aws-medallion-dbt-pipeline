-- dbt will try to create a 'silver' database in Glue
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='id',
    schema='silver' 
) }}

SELECT
    id,
    title,
    category,
    brand,
    CAST(price AS DOUBLE) as price,
    CAST(rating AS DOUBLE) as rating,
    stock,
    availabilityStatus,
    -- This is the timestamp we added in the Lambda function
    from_iso8601_timestamp(extraction_timestamp) as extraction_timestamp
FROM {{ source('dummyjson_api_raw', 'products_raw') }}

{% if is_incremental() %}
  -- Only process data newer than the latest record in this Silver table
  WHERE extraction_timestamp > (SELECT MAX(extraction_timestamp) FROM {{ this }})
{% endif %}