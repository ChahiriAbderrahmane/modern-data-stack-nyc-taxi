{{
    config(
        materialized='view',
        schema='gold'
    )
}}

-- Agrégations journalières pour le dashboard
WITH fact_with_dims AS (
    SELECT
        f.*,
        dt.date AS pickup_date,
        dt.year AS pickup_year,
        dt.month AS pickup_month,
        dt.day AS pickup_day,
        dt.day_of_week_name AS pickup_day_of_week_name,
        dt.day_type,
        dt.time_of_day,
        v.vendorid,
        pt.payment_type
    FROM {{ ref('fact_taxi_trips_v2') }} f 
    LEFT JOIN {{ ref('dim_datetime') }} dt 
        ON f.pickup_datetime_key = dt.datetime_key
    LEFT JOIN {{ ref('dim_vendor') }} v 
        ON f.vendor_key = v.vendor_key
    LEFT JOIN {{ ref('dim_payment_type') }} pt 
        ON f.payment_type_key = pt.payment_type_key
)

SELECT
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_day_of_week_name,
    day_type,
    
    -- Métriques de volume
    COUNT(*) AS total_trips,
    COUNT(DISTINCT vendorid) AS active_vendors,
    SUM(passenger_count_that_day) AS total_passengers,
    
    -- Métriques de distance et durée
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(AVG(avg_speed_mph)::NUMERIC, 2) AS avg_speed_mph,
    
    -- Métriques financières
    ROUND(SUM(total_amount_usd)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount_usd)::NUMERIC, 2) AS avg_fare_usd,
    ROUND(SUM(tip_amount_usd)::NUMERIC, 2) AS total_tips_usd,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_percentage,
    
    -- Distribution par méthode de paiement
    COUNT(CASE WHEN payment_type = 1 THEN 1 END) AS credit_card_trips,
    COUNT(CASE WHEN payment_type = 2 THEN 1 END) AS cash_trips,
    
    -- Distribution par période
    COUNT(CASE WHEN time_of_day = 'Morning' THEN 1 END) AS morning_trips,
    COUNT(CASE WHEN time_of_day = 'Afternoon' THEN 1 END) AS afternoon_trips,
    COUNT(CASE WHEN time_of_day = 'Evening' THEN 1 END) AS evening_trips,
    COUNT(CASE WHEN time_of_day = 'Night' THEN 1 END) AS night_trips,
    
    -- Rush hour ou bien peak (booléen)
    COUNT(CASE WHEN rush_hour_flag = 'Rush Hour' THEN 1 END) AS rush_hour_trips,
    
    -- Airport trips (booléen)
    COUNT(CASE WHEN airport_pickup_flag = 'Pickup at LGA/JFK' THEN 1 END) AS airport_trips,
    ROUND(SUM(CASE WHEN airport_pickup_flag = 'Pickup at LGA/JFK' THEN airport_fee ELSE 0 END)::NUMERIC, 2) AS airport_fees_collected

FROM fact_with_dims

GROUP BY 
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_day_of_week_name,
    day_type
ORDER BY pickup_date DESC