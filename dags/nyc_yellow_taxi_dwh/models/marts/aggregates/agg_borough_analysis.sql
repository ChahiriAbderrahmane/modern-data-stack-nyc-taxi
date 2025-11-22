{{
    config(
        materialized='view',
        schema='gold'
    )
}}

-- Analyse par borough (pickup et dropoff)
WITH fact_with_dims AS (
    SELECT
        f.*,
        loc_pickup.borough AS pickup_borough,
        loc_pickup.zone AS pickup_zone,
        loc_dropoff.borough AS dropoff_borough,
        loc_dropoff.zone AS dropoff_zone,
        dt.year AS pickup_year,
        dt.quarter AS pickup_quarter,
        dt.day_type
    FROM {{ ref('fact_taxi_trips') }} f
    LEFT JOIN {{ ref('dim_location') }} loc_pickup 
        ON f.pickup_location_key = loc_pickup.location_key
    LEFT JOIN {{ ref('dim_location') }} loc_dropoff 
        ON f.dropoff_location_key = loc_dropoff.location_key
    LEFT JOIN {{ ref('dim_datetime') }} dt 
        ON f.pickup_datetime_key = dt.datetime_key
)

SELECT
    pickup_borough,
    dropoff_borough,
    pickup_year,
    pickup_quarter,
    COUNT(*) AS total_trips,

    -- Caractéristiques des trips
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    
    -- Revenue
    ROUND(SUM(total_amount_usd)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount_usd)::NUMERIC, 2) AS avg_fare_usd,
    
    -- Distribution temporelle
    COUNT(CASE WHEN day_type = 'Weekend' THEN 1 END) AS weekend_trips,
    COUNT(CASE WHEN day_type = 'Weekday' THEN 1 END) AS weekday_trips,
    COUNT(CASE WHEN rush_hour_flag = 'Rush Hour' THEN 1 END) AS rush_hour_trips,
    
    -- Top zone de pickup dans ce borough
    MODE() WITHIN GROUP (ORDER BY pickup_zone) AS most_common_pickup_zone,
    
    -- Top zone de dropoff dans ce borough  
    MODE() WITHIN GROUP (ORDER BY dropoff_zone) AS most_common_dropoff_zone

FROM fact_with_dims
WHERE pickup_borough IS NOT NULL
    AND dropoff_borough IS NOT NULL
GROUP BY 
    pickup_borough,
    dropoff_borough,
    pickup_year,
    pickup_quarter
ORDER BY 
    pickup_year DESC,
    pickup_quarter DESC,
    total_trips DESC