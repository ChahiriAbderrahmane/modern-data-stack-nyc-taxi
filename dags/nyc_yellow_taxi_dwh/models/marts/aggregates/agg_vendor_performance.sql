{{ config( materialized='view', schema='gold') }}

-- Analyse de la performance technique et commerciale par Vendeur (CMT vs Curb...)
WITH fact_enriched AS (
    SELECT
        f.total_amount,
        f.tip_amount_usd,
        f.tip_percentage,
        f.trip_distance,
        f.trip_duration_minutes,
        f.avg_speed_mph,
        f.passenger_count,
        f.fare_per_mile,
        f.vendorid,
        f.store_and_fwd_flag, 
        d.year AS pickup_year,
        d.month AS pickup_month,
        tc.distance_category
    FROM {{ ref('fact_taxi_trips_v2') }} f
    LEFT JOIN {{ ref('dim_date') }} d 
        ON f.pickup_date_id = d.date_id
    LEFT JOIN {{ ref('dim_trip_category') }} tc 
        ON f.trip_category_key = tc.trip_category_key
)

SELECT
    vendorid,
    CASE vendorid
        WHEN 1 THEN 'Creative Mobile Technologies (CMT)'
        WHEN 2 THEN 'Curb Mobility'
        WHEN 6 THEN 'Myle Technologies'
        WHEN 7 THEN 'Helix'
        ELSE 'Unknown Vendor'
    END AS vendor_name,
    pickup_year,
    pickup_month,
    -- ANALYSE DE VOLUME & PART DE MARCHÉ
    COUNT(*) AS total_trips,
    -- Part de marché du Vendeur sur ce mois
    ROUND(
        COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY pickup_year, pickup_month) * 100
    , 2) AS market_share_pct,
    -- MÉTRIQUES OPÉRATIONNELLES (Qualité de service)
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(AVG(avg_speed_mph)::NUMERIC, 2) AS avg_speed_mph,
    ROUND(AVG(passenger_count)::NUMERIC, 2) AS avg_passengers,
    -- PERFORMANCE FINANCIÈRE
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare_usd,
    -- Rentabilité kilométrique 
    ROUND(AVG(fare_per_mile)::NUMERIC, 2) AS avg_fare_per_mile,
    -- ANALYSE DES POURBOIRES
    -- Un vendor a-t-il une meilleure interface qui incite plus au tip ?
    ROUND(SUM(tip_amount_usd)::NUMERIC, 2) AS total_tips_usd,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_percentage,
    -- PERFORMANCE TECHNIQUE (Store & Forward)
    -- Si ce chiffre est élevé, le vendor a des problèmes de connexion réseau
    COUNT(CASE WHEN store_and_fwd_flag = 'Y' THEN 1 END) AS store_forward_trips,
    
    ROUND(
        COUNT(CASE WHEN store_and_fwd_flag = 'Y' THEN 1 END)::NUMERIC 
        / NULLIF(COUNT(*), 0)::NUMERIC * 100
    , 2) AS store_forward_pct,
    -- PROFILAGE DES COURSES (Segmentation Distance)
    COUNT(CASE WHEN distance_category = 'Short (< 1 mile)' THEN 1 END) AS short_trips,
    COUNT(CASE WHEN distance_category = 'Medium (1-5 miles)' THEN 1 END) AS medium_trips,
    COUNT(CASE WHEN distance_category = 'Long (5-10 miles)' THEN 1 END) AS long_trips,
    COUNT(CASE WHEN distance_category = 'Very Long (> 10 miles)' THEN 1 END) AS very_long_trips

FROM fact_enriched

GROUP BY 
    vendorid,
    pickup_year,
    pickup_month

ORDER BY 
    pickup_year DESC,
    pickup_month DESC,
    market_share_pct DESC