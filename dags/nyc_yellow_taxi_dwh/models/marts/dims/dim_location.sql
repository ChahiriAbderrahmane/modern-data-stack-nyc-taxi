{{ config(
    materialized='incremental',
    unique_key='location_key', 
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['location_key']}]
) }}

with loc_source_data as (
    SELECT pulocationid,
           pickup_borough,
           pickup_zone,
           dolocationid,
           dropoff_borough,
           dropoff_zone,
           _ingestion_timestamp
    FROM {{ ref('nyc_tripdata_2024_v2') }}

    {% if is_incremental() %}
        WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

unioned_locations as (
    SELECT 
        pulocationid as locationid,
        pickup_borough as borough,
        pickup_zone as zone,
        _ingestion_timestamp
    FROM loc_source_data
    WHERE pulocationid is not null

    UNION ALL

    SELECT 
        dolocationid as locationid,
        dropoff_borough as borough,
        dropoff_zone as zone,
        _ingestion_timestamp
    FROM loc_source_data
    WHERE dolocationid IS NOT NULL
),

unique_locations AS (
    SELECT 
        locationid,
        borough,
        zone,
        MAX(_ingestion_timestamp) as _ingestion_timestamp
    FROM unioned_locations
    GROUP BY 1, 2, 3
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['locationid']) }} as location_key,
    locationid,
    borough,
    zone,
    _ingestion_timestamp
FROM unique_locations