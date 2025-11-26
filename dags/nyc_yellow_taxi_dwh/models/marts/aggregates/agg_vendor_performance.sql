{{
    config(
        materialized='view',
        schema='gold'
    )
}}

-- Performance par vendeur
WITH fact_with_dims AS (
    SELECT
        f.*,
        v.vendor_key AS dim_vendor_key,
        v.vendorid,
        v.vendor_name,
        dt.year AS pickup_year,
        dt.month AS pickup_month,
        sf.store_and_fwd_flag,
        tc.distance_category
    FROM {{ ref('fact_taxi_trips_v2') }} f
    LEFT JOIN {{ ref('dim_vendor') }} v ON f.vendor_key = v.vendor_key
    LEFT JOIN {{ ref('dim_datetime') }} dt ON f.pickup_datetime_key = dt.datetime_key
    LEFT JOIN {{ ref('dim_store_forward') }} sf ON f.store_forward_key = sf.store_forward_key
    LEFT JOIN {{ ref('dim_trip_category') }} tc ON f.trip_category_key = tc.trip_category_key
)

SELECT
    vendor_key,
    vendorid,
    vendor_name,
    pickup_year,
    pickup_month,
    
    -- Volume
    COUNT(*) AS total_trips,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY pickup_year, pickup_month) * 100, 2) AS market_share_pct,
    
    -- Métriques opérationnelles
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(AVG(avg_speed_mph)::NUMERIC, 2) AS avg_speed_mph,
    ROUND(AVG(passenger_count_that_day)::NUMERIC, 2) AS avg_passengers,
    
    -- Performance financière
    ROUND(SUM(total_amount_usd)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount_usd)::NUMERIC, 2) AS avg_fare_usd,
    ROUND(AVG(fare_per_mile)::NUMERIC, 2) AS avg_fare_per_mile,
    
    -- Pourboires
    ROUND(SUM(tip_amount_usd)::NUMERIC, 2) AS total_tips_usd,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_percentage,
    
    -- Efficacité
    COUNT(CASE WHEN store_and_fwd_flag = 'Y' THEN 1 END) AS store_forward_trips,
    ROUND(COUNT(CASE WHEN store_and_fwd_flag = 'Y' THEN 1 END)::NUMERIC / NULLIF(COUNT(*), 0)::NUMERIC * 100, 2) AS store_forward_pct,
    
    -- Distribution des courses
    COUNT(CASE WHEN distance_category = 'Short (< 1 mile)' THEN 1 END) AS short_trips,
    COUNT(CASE WHEN distance_category = 'Medium (1-5 miles)' THEN 1 END) AS medium_trips,
    COUNT(CASE WHEN distance_category = 'Long (5-10 miles)' THEN 1 END) AS long_trips,
    COUNT(CASE WHEN distance_category = 'Very Long (> 10 miles)' THEN 1 END) AS very_long_trips

FROM fact_with_dims
GROUP BY 
    vendor_key,
    vendorid,
    vendor_name,
    pickup_year,
    pickup_month
ORDER BY 
    pickup_year DESC,
    pickup_month DESC,
    total_trips DESC