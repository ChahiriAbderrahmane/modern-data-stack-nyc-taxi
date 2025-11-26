{{
    config(
        materialized='view',
        schema='gold'
    )
}}

-- Patterns horaires pour identifier les pics de demande
WITH fact_with_dims AS (
    SELECT
        f.*,
        dt.hour AS pickup_hour,
        dt.time_of_day,
        dt.day_type,
        dt.year AS pickup_year,
        dt.month AS pickup_month,
        loc.borough AS pickup_borough
    FROM {{ ref('fact_taxi_trips_v2') }} f
    LEFT JOIN {{ ref('dim_datetime') }} dt 
        ON f.pickup_datetime_key = dt.datetime_key
    LEFT JOIN {{ ref('dim_location') }} loc 
        ON f.pickup_location_key = loc.location_key
)

SELECT
    pickup_hour,
    time_of_day,
    day_type,
    rush_hour_flag,
    pickup_year,
    pickup_month,
    
    -- Volume
    COUNT(*) AS total_trips,
    ROUND(AVG(COUNT(*)) OVER (PARTITION BY pickup_hour, day_type, pickup_year, pickup_month), 0) AS avg_trips_per_hour,
    
    -- Métriques de performance
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(AVG(avg_speed_mph)::NUMERIC, 2) AS avg_speed_mph,
    
    -- Revenue
    ROUND(SUM(total_amount_usd)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount_usd)::NUMERIC, 2) AS avg_fare_usd,
    
    -- Efficacité
    ROUND(AVG(fare_per_minute)::NUMERIC, 2) AS avg_fare_per_minute,
    
    -- Top borough pour cette heure
    MODE() WITHIN GROUP (ORDER BY pickup_borough) AS most_common_pickup_borough

FROM fact_with_dims
GROUP BY 
    pickup_hour,
    time_of_day,
    day_type,
    rush_hour_flag,
    pickup_year,
    pickup_month
ORDER BY 
    pickup_year DESC,
    pickup_month DESC,
    day_type,
    pickup_hour