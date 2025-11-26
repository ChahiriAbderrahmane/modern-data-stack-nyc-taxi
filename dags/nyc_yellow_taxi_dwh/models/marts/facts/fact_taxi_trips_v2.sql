{{ config(
    materialized='incremental',
    unique_key='trip_key', 
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['trip_key']}]
) }}

with silver_source as (
    select * from {{ ref('nyc_tripdata_2024_v2') }} 
),

last_time AS (
    {% if is_incremental() %}
        SELECT COALESCE(MAX(_ingestion_timestamp), '1900-01-01'::TIMESTAMP) as max_ts
        FROM {{ this }}
    {% else %}
        -- Si c'est le premier run (Full Refresh), on prend une date très vieille
        SELECT '1900-01-01'::TIMESTAMP as max_ts
    {% endif %}
),

incremental_source AS (
    SELECT s.* FROM silver_source s
    CROSS JOIN last_time w
    -- On filtre simplement sur la colonne calculée
    WHERE s._ingestion_timestamp > w.max_ts
),

fact_staged AS (
    SELECT 
        *,
        -- les clés de jointure vers les dimensions
        {{ dbt_utils.generate_surrogate_key(['vendorid']) }} as vendor_key,
        {{ dbt_utils.generate_surrogate_key(['tpep_pickup_datetime']) }} as pickup_datetime_key,
        {{ dbt_utils.generate_surrogate_key(['tpep_dropoff_datetime']) }} as dropoff_datetime_key,
        {{ dbt_utils.generate_surrogate_key(['pulocationid']) }} as pickup_location_key,
        {{ dbt_utils.generate_surrogate_key(['dolocationid']) }} as dropoff_location_key,
        {{ dbt_utils.generate_surrogate_key(['RatecodeID_clean']) }} as rate_code_key,
        {{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_key,
        {{ dbt_utils.generate_surrogate_key(['store_and_fwd_flag_cleaned']) }} as store_forward_key,
        {{ dbt_utils.generate_surrogate_key(['distance_category', 'duration_category', 'fare_category']) }} as trip_category_key
    FROM incremental_source
),

deduplicated_data AS (
    SELECT DISTINCT ON (trip_key) *
    FROM fact_staged
    ORDER BY trip_key, _ingestion_timestamp DESC
)

SELECT
    -- Primary Key
    trip_key,
    
    -- Foreign keys to the other dimensions
    vendor_key,
    pickup_datetime_key,
    dropoff_datetime_key,
    pickup_location_key,
    dropoff_location_key,
    rate_code_key,
    payment_type_key,
    store_forward_key,
    trip_category_key,
    
    -- Degenerate dimensions (flags)
    airport_pickup_flag,
    data_quality_flag,
    is_invalid_trip,
    rush_hour_flag,
    
    -- Metrics 
    passenger_count_that_day,
    passenger_count_quality,
    trip_duration_minutes,
    trip_distance,
    avg_speed_mph,
    
    -- Fare measures
    base_fare_usd,
    surcharges_usd,
    mta_tax_usd,
    tip_amount_usd,
    tolls_amount_usd,
    improvement_surcharge,
    total_amount_usd,
    congestion_surcharge,
    airport_fee,
    revenue_amount,
    fare_per_mile,
    fare_per_minute,
    tip_percentage,
    _source_filename,
    _ingestion_timestamp

FROM deduplicated_data s