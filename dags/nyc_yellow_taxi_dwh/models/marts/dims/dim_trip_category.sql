{{ config(
    materialized='incremental',
    unique_key='trip_category_key',
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['trip_category_key']}]
) }}

WITH source_data AS (
    SELECT 
        distance_category,
        duration_category,
        fare_category,
        _ingestion_timestamp
    FROM {{ ref('nyc_tripdata_2024_v2') }}
    WHERE (distance_category IS NOT NULL 
           OR duration_category IS NOT NULL 
           OR fare_category IS NOT NULL)

    {% if is_incremental() %}
        AND _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

unique_categories AS (
    SELECT 
        distance_category,
        duration_category,
        fare_category,
        MAX(_ingestion_timestamp) as _ingestion_timestamp
    FROM source_data
    GROUP BY 1, 2, 3
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['distance_category', 'duration_category', 'fare_category']) }} as trip_category_key,
    distance_category,
    duration_category,
    fare_category,
    _ingestion_timestamp 
FROM unique_categories