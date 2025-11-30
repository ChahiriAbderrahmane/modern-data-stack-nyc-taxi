{{ config( materialized='view', schema='gold') }}

-- Patterns horaires pour identifier les pics de demande
WITH fact_with_dims AS (
    SELECT
        f.total_amount,
        f.trip_distance,
        f.trip_duration_minutes,
        f.avg_speed_mph,
        f.fare_per_minute,
        t.hour AS pickup_hour,
        t.time_of_day,
        t.traffic_peak_period, -- 'Morning Peak', 'Evening Peak', 'Off-Peak'
        d.year AS pickup_year,
        d.month AS pickup_month,
        d.day_type,      -- 'Weekday' vs 'Weekend'
        d.full_date,     -- Nécessaire pour compter le nombre de jours distincts
        loc.borough AS pickup_borough
    FROM {{ ref('fact_taxi_trips_v2') }} f
    LEFT JOIN {{ ref('dim_time') }} t 
        ON f.pickup_time_id = t.time_id
    LEFT JOIN {{ ref('dim_date') }} d 
        ON f.pickup_date_id = d.date_id
    LEFT JOIN {{ ref('dim_location') }} loc 
        ON f.pulocationid = loc.locationid
)

SELECT
    pickup_year,
    pickup_month,
    day_type,         -- Permet de comparer "Lundi 8h" vs "Samedi 8h"
    pickup_hour,
    time_of_day,
    CASE 
        WHEN traffic_peak_period IN ('Morning Peak', 'Evening Peak') AND day_type = 'Weekday' 
        THEN 'Rush Hour' 
        ELSE 'Normal Flow' 
    END AS traffic_context,
    COUNT(*) AS total_trips,
    -- CALCUL DE LA MOYENNE HORAIRE 
    -- Total des courses / Nombre de jours distincts dans cette période
    -- Ex: S'il y a 1000 courses à 8h00 sur 20 jours ouvrés -> Moyenne = 50/heure
    ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT full_date), 0)::NUMERIC, 0) AS avg_daily_trips_at_this_hour,
    -- PERFORMANCE
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(AVG(avg_speed_mph)::NUMERIC, 2) AS avg_speed_mph,
    -- REVENUS
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare_usd,
    -- EFFICACITÉ (Rentabilité pour le chauffeur)
    ROUND(AVG(fare_per_minute)::NUMERIC, 2) AS avg_fare_per_minute,
    -- TOP BOROUGH (Statistique)
    -- "Quel est le quartier le plus actif à cette heure-là ?"
    MODE() WITHIN GROUP (ORDER BY pickup_borough) AS most_common_pickup_borough
FROM fact_with_dims

GROUP BY 
    pickup_year,
    pickup_month,
    day_type,
    pickup_hour,
    time_of_day,
    traffic_peak_period -- Nécessaire pour le CASE du traffic_context

ORDER BY 
    pickup_year DESC,
    pickup_month DESC,
    day_type,
    pickup_hour