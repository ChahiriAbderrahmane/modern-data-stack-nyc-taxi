{{
  config(
    materialized='table'
  )
}}

WITH staging AS (
    SELECT * FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
),

vendor_dim AS (
    SELECT * FROM {{ ref('dim_vendor') }}
),

datetime_dim AS (
    SELECT * FROM {{ ref('dim_datetime') }}
),

location_dim AS (
    SELECT * FROM {{ ref('dim_location') }}
),

rate_code_dim AS (
    SELECT * FROM {{ ref('dim_rate_code') }}
),

payment_type_dim AS (
    SELECT * FROM {{ ref('dim_payment_type') }}
),

store_forward_dim AS (
    SELECT * FROM {{ ref('dim_store_forward') }}
),

trip_category_dim AS (
    SELECT * FROM {{ ref('dim_trip_category') }}
)

SELECT
    -- Surrogate key (Primary Key)
    {{ dbt_utils.generate_surrogate_key(['s.vendorid', 's.tpep_pickup_datetime', 's.tpep_dropoff_datetime', 's.pulocationid', 's.dolocationid']) }} as trip_key,
    
    -- Foreign keys to the other dimensions
    v.vendor_key,
    dt_pickup.datetime_key as pickup_datetime_key,
    dt_dropoff.datetime_key as dropoff_datetime_key,
    loc_pickup.location_key as pickup_location_key,
    loc_dropoff.location_key as dropoff_location_key,
    rc.rate_code_key,
    pt.payment_type_key,
    sf.store_forward_key,
    tc.trip_category_key,
    
    -- Degenerate dimensions (flags)
    s.airport_pickup_flag,
    s.data_quality_flag,
    s.is_invalid_trip,
    s.rush_hour_flag,
    
    -- Metrics 
    s.passenger_count_that_day,
    s.passenger_count_quality,
    s.trip_duration_minutes,
    s.trip_distance,
    s.avg_speed_mph,
    
    -- Fare measures
    s.base_fare_usd,
    s.surcharges_usd,
    s.mta_tax_usd,
    s.tip_amount_usd,
    s.tolls_amount_usd,
    s.improvement_surcharge,
    s.total_amount_usd,
    s.congestion_surcharge,
    s.airport_fee,
    s.revenue_amount,
    s.fare_per_mile,
    s.fare_per_minute,
    s.tip_percentage

FROM staging s
LEFT JOIN vendor_dim v 
    ON {{ dbt_utils.generate_surrogate_key(['s.vendorid']) }} = v.vendor_key
LEFT JOIN datetime_dim dt_pickup 
    ON {{ dbt_utils.generate_surrogate_key(['s.tpep_pickup_datetime']) }} = dt_pickup.datetime_key
LEFT JOIN datetime_dim dt_dropoff 
    ON {{ dbt_utils.generate_surrogate_key(['s.tpep_dropoff_datetime']) }} = dt_dropoff.datetime_key
LEFT JOIN location_dim loc_pickup 
    ON {{ dbt_utils.generate_surrogate_key(['s.pulocationid']) }} = loc_pickup.location_key
LEFT JOIN location_dim loc_dropoff 
    ON {{ dbt_utils.generate_surrogate_key(['s.dolocationid']) }} = loc_dropoff.location_key
LEFT JOIN rate_code_dim rc 
    ON {{ dbt_utils.generate_surrogate_key(['s.ratecodeid']) }} = rc.rate_code_key
LEFT JOIN payment_type_dim pt 
    ON {{ dbt_utils.generate_surrogate_key(['s.payment_type']) }} = pt.payment_type_key
LEFT JOIN store_forward_dim sf 
    ON {{ dbt_utils.generate_surrogate_key(['s.store_and_fwd_flag']) }} = sf.store_forward_key
LEFT JOIN trip_category_dim tc 
    ON {{ dbt_utils.generate_surrogate_key(['s.distance_category', 's.duration_category', 's.fare_category']) }} = tc.trip_category_key