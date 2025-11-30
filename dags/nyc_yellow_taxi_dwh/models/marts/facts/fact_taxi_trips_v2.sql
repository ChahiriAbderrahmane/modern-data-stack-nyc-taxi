{{ config(
    materialized='incremental',
    unique_key='trip_key', 
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['trip_key']}, {'columns': ['pickup_date_id']}, {'columns': ['pulocationid']}]
) }}

with silver_source as (
    select * from {{ ref('nyc_tripdata_2024_v2') }} 
    -- === FILTRE STRICT DE QUALITÉ ===
    -- Seules les données valides entrent dans le Data Warehouse Final
    WHERE data_quality_flag = 'Valid'
),

incremental_source AS (
    SELECT * FROM silver_source
    {% if is_incremental() %}
        -- On ne prend que les lignes arrivées après la dernière exécution de CE modèle
        WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

fact_staged AS (
    SELECT 
        *,
        -- les clés de jointure vers les dimensions
        {{ dbt_utils.generate_surrogate_key(['data_quality_flag', 'distance_category', 'duration_category', 'fare_category']) }} as trip_category_key
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
    vendorid,
    pickup_date_id,
    pickup_time_id,
    dropoff_date_id,
    dropoff_time_id,
    pulocationid,
    dolocationid,
    ratecodeid,
    payment_type_key as payment_type_id,
    trip_category_key,

    -- Degenerate dimensions (flags)
    store_and_fwd_flag,
    airport_pickup_flag,
    data_quality_flag,   
    is_invalid_trip,

    -- Metrics
    passenger_count_in_trip as passenger_count,
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
    airport_fee,
    congestion_surcharge_cleaned as congestion_surcharge,
    total_amount_calculated as total_amount,
    revenue_amount,
    fare_per_mile,
    fare_per_minute,
    tip_percentage,
    total_amount_usd as total_amount_before_corrections,

    _source_filename,
    _ingestion_timestamp

FROM deduplicated_data s