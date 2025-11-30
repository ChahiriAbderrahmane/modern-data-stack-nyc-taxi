{{ config( materialized='view', schema='gold') }}

WITH fact_enriched AS (
    SELECT
        f.total_amount,    
        f.trip_distance,
        f.trip_duration_minutes,
        loc_pickup.borough AS pickup_borough,
        loc_pickup.zone AS pickup_zone,
        loc_dropoff.borough AS dropoff_borough,
        loc_dropoff.zone AS dropoff_zone,
        d.year AS pickup_year,
        d.quarter AS pickup_quarter,
        d.day_type, -- 'Weekend' ou 'Weekday'
        t.traffic_peak_period -- 'Morning Peak', 'Evening Peak', 'Off-Peak'

    FROM {{ ref('fact_taxi_trips_v2') }} f
    -- Jointure Date
    LEFT JOIN {{ ref('dim_date') }} d 
        ON f.pickup_date_id = d.date_id  
    -- Jointure Temps 
    LEFT JOIN {{ ref('dim_time') }} t 
        ON f.pickup_time_id = t.time_id 
    -- Jointures Lieux 
    LEFT JOIN {{ ref('dim_location') }} loc_pickup 
        ON f.pulocationid = loc_pickup.locationid
    LEFT JOIN {{ ref('dim_location') }} loc_dropoff 
        ON f.dolocationid = loc_dropoff.locationid
)

SELECT
    pickup_borough,
    dropoff_borough,
    pickup_year,
    pickup_quarter,
    COUNT(*) AS total_trips,
    -- 3. MÉTRIQUES DE VOYAGE (Moyennes)
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    -- 4. REVENUS (Totals et Moyennes)
    -- On utilise SUM pour le CA total, c'est crucial pour le business
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare_usd,
    -- 5. DISTRIBUTION TEMPORELLE 
    COUNT(CASE WHEN day_type = 'Weekend' THEN 1 END) AS weekend_trips,
    COUNT(CASE WHEN day_type = 'Weekday' THEN 1 END) AS weekday_trips,
    -- On compte les trajets en heure de pointe (Matin ou Soir)
    COUNT(CASE WHEN traffic_peak_period IN ('Morning Peak', 'Evening Peak') THEN 1 END) AS rush_hour_trips,
    -- 6. STATISTIQUES AVANCÉES (Mode = Valeur la plus fréquente)
    -- Cela répond à "Quelle est la zone la plus populaire dans ce borough ?"
    MODE() WITHIN GROUP (ORDER BY pickup_zone) AS most_common_pickup_zone,
    MODE() WITHIN GROUP (ORDER BY dropoff_zone) AS most_common_dropoff_zone

FROM fact_enriched
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
    total_revenue_usd DESC 