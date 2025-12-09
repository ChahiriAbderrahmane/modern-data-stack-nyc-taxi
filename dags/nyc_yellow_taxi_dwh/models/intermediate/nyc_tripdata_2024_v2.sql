{{ config(
    materialized='incremental',
    unique_key='trip_key', 
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['trip_key']}] 
) }}
-- L'index est maintenu, donc on doit garantir l'unicité avant !
with src as (
    select * from {{ source('bronze_data', 'bronze_taxi_trips') }}
),

incremental_source AS (
    SELECT 
        CASE 
            WHEN vendorid IN (1, 2, 6, 7) THEN vendorid
            ELSE 99 
        END as vendorid_,
        * FROM src
    {% if is_incremental() %}
        -- On ne prend que les lignes arrivées après la dernière exécution
        WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

base_data AS (
 select 
    {{ dbt_utils.generate_surrogate_key(['vendorid_', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pulocationid', 'dolocationid']) }} as trip_key, 
    vendorid_ as vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    COALESCE(fare_amount, 0) as fare_amount,
    COALESCE(extra, 0) as extra,
    COALESCE(mta_tax, 0) as mta_tax,
    COALESCE(tip_amount, 0) as tip_amount,
    COALESCE(tolls_amount, 0) as tolls_amount,
    COALESCE(improvement_surcharge, 0) as improvement_surcharge,
    total_amount, 
    COALESCE(congestion_surcharge, 0) as congestion_surcharge,
    COALESCE(airport_fee, 0) as airport_fee,
    COALESCE(passenger_count, 0) as passenger_count,
    trip_distance,
    case
        WHEN RatecodeID IN (1, 2, 3, 4, 5, 6) THEN RatecodeID
        Else 99
    END AS RatecodeID,
    COALESCE(store_and_fwd_flag,'N') as store_and_fwd_flag,
    COALESCE(PULocationID, 264) as PULocationID,
    COALESCE(DOLocationID, 264)as DOLocationID,
    case 
        when payment_type in (0,1,2,3,4,5,6) then payment_type
        else 99
    end as payment_type,
    ROUND((EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60),2)::FLOAT AS trip_duration_minutes,
    _source_filename,
    _ingestion_timestamp
from incremental_source),

-- === DÉDUPLICATION ===
deduplicated_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_key 
            ORDER BY _ingestion_timestamp DESC -- En cas de doublon, on garde le fichier le plus récent
        ) as row_num
    FROM base_data
)

SELECT 
    bd.trip_key, 
    bd.vendorid, 
    case 
        when bd.vendorid = 1 then 'Creative Mobile Technologies, LLC'
        when bd.vendorid = 2 then 'Curb Mobility, LLC'
        when bd.vendorid = 6 then 'Myle Technologies Inc'
        when bd.vendorid = 7 then 'Helix' 
        ELSE 'Other' 
    end as vendor_name,

    bd.tpep_pickup_datetime,
    CAST(TO_CHAR(tpep_pickup_datetime, 'YYYYMMDD') AS INT) as pickup_date_id,
    (EXTRACT(HOUR FROM tpep_pickup_datetime) * 100) + EXTRACT(MINUTE FROM tpep_pickup_datetime) as pickup_time_id,
    bd.tpep_dropoff_datetime,
    CAST(TO_CHAR(tpep_dropoff_datetime, 'YYYYMMDD') AS INT) as dropoff_date_id,
    (EXTRACT(HOUR FROM tpep_dropoff_datetime) * 100) + EXTRACT(MINUTE FROM tpep_dropoff_datetime) as dropoff_time_id,

    bd.passenger_count as passenger_count_in_trip,
    CASE 
        WHEN bd.passenger_count = 0 THEN 'Invalid - Zero passengers'
        WHEN bd.passenger_count > 6 THEN 'Suspicious - Too many'
        ELSE 'Valid'
    END AS passenger_count_quality,
    bd.trip_duration_minutes,
    bd.trip_distance, 
    bd.ratecodeid,
    bd.store_and_fwd_flag,
    CASE
        WHEN bd.store_and_fwd_flag = 'Y' THEN 'Store and forward trip'
        WHEN bd.store_and_fwd_flag = 'N' THEN 'Not a store and forward trip'
        ELSE 'Unknown'
    END AS store_and_fwd_description,

    bd.pulocationid,
    bd.dolocationid,
    bd.payment_type as payment_type_key, 
    bd.fare_amount as base_fare_usd, 
    bd.extra as surcharges_usd,
    bd.mta_tax AS mta_tax_usd, 
    bd.tip_amount as tip_amount_usd, 
    bd.tolls_amount as tolls_amount_usd, 
    bd.improvement_surcharge,
    bd.airport_fee,
    bd.total_amount as total_amount_usd, 
    
    CASE
        WHEN bd.congestion_surcharge = 0 
             AND ROUND((bd.total_amount - (bd.fare_amount + bd.extra + bd.mta_tax + bd.tip_amount + bd.tolls_amount + bd.improvement_surcharge + bd.congestion_surcharge + bd.airport_fee))::numeric, 2) = 2.5 
        THEN 2.5
        ELSE bd.congestion_surcharge
    END AS congestion_surcharge_cleaned,

    (
        bd.fare_amount + 
        bd.extra + 
        bd.mta_tax + 
        bd.tip_amount + 
        bd.tolls_amount + 
        bd.improvement_surcharge + 
        bd.airport_fee +
        CASE
            WHEN bd.congestion_surcharge = 0 
                 AND ROUND((bd.total_amount - (bd.fare_amount + bd.extra + bd.mta_tax + bd.tip_amount + bd.tolls_amount + bd.improvement_surcharge + bd.congestion_surcharge + bd.airport_fee))::numeric, 2) = 2.5 
            THEN 2.5
            ELSE bd.congestion_surcharge
        END
    ) AS total_amount_calculated, 

    CASE 
        WHEN bd.airport_fee > 0 THEN 'Pickup at LGA/JFK'
        ELSE 'Other location'
    END AS airport_pickup_flag,

    CASE 
        WHEN bd.trip_duration_minutes < 0 THEN 'Invalid - Negative duration'
        WHEN bd.trip_duration_minutes = 0 THEN 'Invalid - Zero duration'
        WHEN bd.trip_duration_minutes > 480 THEN 'Suspicious - Over 8h'
        WHEN bd.trip_distance = 0 AND bd.trip_duration_minutes > 5 THEN 'Suspicious - No distance'
        WHEN bd.passenger_count > 6 THEN 'Invalid - Passenger count'
        WHEN bd.fare_amount < 0 THEN 'Invalid - Negative fare'
        WHEN bd.total_amount < 0 THEN 'Invalid - Negative total'
        ELSE 'Valid'
    END AS data_quality_flag,
    CASE 
        WHEN bd.tpep_dropoff_datetime <= bd.tpep_pickup_datetime THEN TRUE
        ELSE FALSE
    END AS is_invalid_trip,
    CASE 
        WHEN bd.trip_duration_minutes > 0 THEN ROUND((bd.trip_distance / (bd.trip_duration_minutes / 60.0))::NUMERIC, 2)
        ELSE NULL
    END AS avg_speed_mph, 
    CASE 
        WHEN bd.trip_distance > 0 THEN ROUND((bd.fare_amount / bd.trip_distance)::NUMERIC, 2)
        ELSE NULL
    END AS fare_per_mile,
    CASE 
        WHEN bd.trip_duration_minutes > 0 THEN ROUND((bd.fare_amount / bd.trip_duration_minutes)::NUMERIC, 2)
        ELSE NULL
    END AS fare_per_minute,
    CASE 
        WHEN bd.fare_amount > 0 THEN ROUND((bd.tip_amount / bd.fare_amount * 100)::NUMERIC, 2)
        ELSE 0
    END AS tip_percentage,

    (bd.fare_amount + bd.tip_amount + bd.tolls_amount) AS revenue_amount,
    CASE 
        WHEN ABS(bd.trip_distance) < 1 THEN 'Short (< 1 mile)'
        WHEN ABS(bd.trip_distance) BETWEEN 1 AND 5 THEN 'Medium (1-5 miles)'
        WHEN ABS(bd.trip_distance) BETWEEN 5 AND 10 THEN 'Long (5-10 miles)'
        WHEN ABS(bd.trip_distance) > 10 THEN 'Very Long (> 10 miles)'
        ELSE 'Unknown'
    END AS distance_category,
    CASE 
        WHEN ABS(bd.trip_duration_minutes) < 5 THEN 'Very Short (< 5 min)'
        WHEN ABS(bd.trip_duration_minutes) BETWEEN 5 AND 15 THEN 'Short (5-15 min)'
        WHEN ABS(bd.trip_duration_minutes) BETWEEN 15 AND 30 THEN 'Medium (15-30 min)'
        WHEN ABS(bd.trip_duration_minutes) BETWEEN 30 AND 60 THEN 'Long (30-60 min)'
        WHEN ABS(bd.trip_duration_minutes) > 60 THEN 'Very Long (> 60 min)'
        ELSE 'Invalid'
    END AS duration_category,
    CASE 
        WHEN bd.total_amount < 10 THEN 'Low (< $10)'
        WHEN bd.total_amount BETWEEN 10 AND 25 THEN 'Medium ($10-$25)'
        WHEN bd.total_amount BETWEEN 25 AND 50 THEN 'High ($25-$50)'
        WHEN bd.total_amount > 50 THEN 'Very High (> $50)'
        ELSE 'Invalid'
    END AS fare_category,
    DATE(bd.tpep_pickup_datetime) AS pickup_date,
    DATE(bd.tpep_dropoff_datetime) AS dropoff_date,
    bd._source_filename,
    bd._ingestion_timestamp

FROM deduplicated_data bd 
WHERE bd.row_num = 1 -- ON NE GARDE QUE L'UNIQUE
    AND EXTRACT(YEAR FROM bd.tpep_pickup_datetime) IN (2023, 2024)
    AND total_amount > 0 
    AND total_amount < 5000 
    AND trip_distance < 1000 
    AND passenger_count < 7