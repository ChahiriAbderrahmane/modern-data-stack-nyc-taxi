{{
    config(
        materialized='view',
        schema='gold'
    )
}}

-- Analyse des méthodes de paiement
WITH fact_with_dims AS (
    SELECT
        f.*,
        pt.payment_type,
        pt.payment_type_description,
        dt.year AS pickup_year,
        dt.month AS pickup_month,
        tc.fare_category
    FROM {{ ref('fact_taxi_trips_v2') }} f
    LEFT JOIN {{ ref('dim_payment_type') }} pt ON f.payment_type_key = pt.payment_type_key
    LEFT JOIN {{ ref('dim_datetime') }} dt ON f.pickup_datetime_key = dt.datetime_key
    LEFT JOIN {{ ref('dim_trip_category') }} tc ON f.trip_category_key = tc.trip_category_key
)

SELECT
    payment_type,
    payment_type_description,
    pickup_year,
    pickup_month,
    
    -- Volume
    COUNT(*) AS total_trips,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY pickup_year, pickup_month) * 100, 2) AS payment_method_share_pct,
    
    -- Revenus
    ROUND(SUM(total_amount_usd)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount_usd)::NUMERIC, 2) AS avg_fare_usd,
    
    -- Pourboires (principalement pour cartes de crédit)
    ROUND(SUM(tip_amount_usd)::NUMERIC, 2) AS total_tips_usd,
    ROUND(AVG(tip_amount_usd)::NUMERIC, 2) AS avg_tip_usd,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_percentage,
    
    -- Caractéristiques des trips
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    
    -- Distribution par catégorie de montant
    COUNT(CASE WHEN fare_category = 'Low (< $10)' THEN 1 END) AS low_fare_trips,
    COUNT(CASE WHEN fare_category = 'Medium ($10-$25)' THEN 1 END) AS medium_fare_trips,
    COUNT(CASE WHEN fare_category = 'High ($25-$50)' THEN 1 END) AS high_fare_trips,
    COUNT(CASE WHEN fare_category = 'Very High (> $50)' THEN 1 END) AS very_high_fare_trips

FROM fact_with_dims
GROUP BY 
    payment_type,
    payment_type_description,
    pickup_year,
    pickup_month
ORDER BY 
    pickup_year DESC,
    pickup_month DESC,
    total_trips DESC