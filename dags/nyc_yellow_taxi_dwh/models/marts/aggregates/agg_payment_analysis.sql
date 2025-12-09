{{ config(
    materialized='view',
    schema='gold',
    tags=['bi_reporting']
) }}

-- Analyse des méthodes de paiement et de la générosité (Pourboires)
WITH fact_enriched AS (
    SELECT
        f.total_amount,        -- Montant calculé propre 
        f.tip_amount_usd,
        f.tip_percentage,
        f.trip_distance,
        f.trip_duration_minutes,
        f.payment_type_id,
        d.year AS pickup_year,
        d.month AS pickup_month,
        tc.fare_category
    FROM {{ ref('fact_taxi_trips_v2') }} f
    LEFT JOIN {{ ref('dim_date') }} d 
        ON f.pickup_date_id = d.date_id
    LEFT JOIN {{ ref('dim_trip_category') }} tc 
        ON f.trip_category_key = tc.trip_category_key
)

SELECT
    payment_type_id,
    CASE payment_type_id
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Unknown/Other'
    END AS payment_type_description,
    
    pickup_year,
    pickup_month,
    
    -- 1. VOLUME & PARTS DE MARCHÉ
    COUNT(*) AS total_trips,
    
    -- Le calcul de part de marché 
    ROUND(
        COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY pickup_year, pickup_month) * 100
    , 2) AS payment_method_share_pct,
    
    -- 2. REVENUS
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare_usd,
    
    -- 3. ANALYSE DES POURBOIRES (Tips)
    ROUND(SUM(tip_amount_usd)::NUMERIC, 2) AS total_tips_usd,
    ROUND(AVG(tip_amount_usd)::NUMERIC, 2) AS avg_tip_usd,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_percentage,
    
    -- 4. PROFILAGE (Distances/Durées par moyen de paiement)
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    
    -- 5. SEGMENTATION PAR MONTANT (Pivot)
    -- Ces colonnes fonctionnent car fare_category n'est PAS dans le GROUP BY
    COUNT(CASE WHEN fare_category = 'Low (< $10)' THEN 1 END) AS low_fare_trips,
    COUNT(CASE WHEN fare_category = 'Medium ($10-$25)' THEN 1 END) AS medium_fare_trips,
    COUNT(CASE WHEN fare_category = 'High ($25-$50)' THEN 1 END) AS high_fare_trips,
    COUNT(CASE WHEN fare_category = 'Very High (> $50)' THEN 1 END) AS very_high_fare_trips

FROM fact_enriched

GROUP BY 
    payment_type_id,
    -- On répète le CASE pour le GROUP BY (standard SQL)
    CASE payment_type_id
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Unknown/Other'
    END,
    pickup_year,
    pickup_month

ORDER BY 
    pickup_year DESC,
    pickup_month DESC,
    total_trips DESC