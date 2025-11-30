{{ config( materialized='view', schema='gold') }}

-- Agrégations journalières pour le dashboard
WITH daily_data_joined AS (
    SELECT
        d.full_date AS pickup_date,
        d.year AS pickup_year,
        d.month AS pickup_month,
        d.day_of_month AS pickup_day,
        d.day_name AS pickup_day_of_week_name,
        d.day_type, -- 'Weekend' / 'Weekday'

        t.time_of_day, -- 'Morning', 'Afternoon'...
        t.traffic_peak_period, -- 'Morning Peak', 'Evening Peak'...

        f.vendorid,
        f.payment_type_id,
        f.passenger_count,
        f.trip_distance,
        f.trip_duration_minutes,
        f.avg_speed_mph,
        f.total_amount,      -- Le montant calculé propre
        f.tip_amount_usd,
        f.tip_percentage,
        f.airport_pickup_flag,
        f.airport_fee

    FROM {{ ref('fact_taxi_trips_v2') }} f 
    
    -- Jointure Calendrier
    LEFT JOIN {{ ref('dim_date') }} d 
        ON f.pickup_date_id = d.date_id
        
    -- Jointure Horloge (Crucial pour ton analyse horaire)
    LEFT JOIN {{ ref('dim_time') }} t 
        ON f.pickup_time_id = t.time_id
)

SELECT
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_day_of_week_name,
    day_type,
    COUNT(*) AS total_trips,
    -- Combien de vendors différents ont travaillé ce jour-là ?
    COUNT(DISTINCT vendorid) AS active_vendors,
    SUM(passenger_count) AS total_passengers,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    ROUND(AVG(avg_speed_mph)::NUMERIC, 2) AS avg_speed_mph,
    -- === MÉTRIQUES FINANCIÈRES ===
    -- Revenu Total (Le KPI le plus important)
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue_usd,
    -- Panier moyen
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare_usd,
    -- Pourboires
    ROUND(SUM(tip_amount_usd)::NUMERIC, 2) AS total_tips_usd,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_percentage,
    -- === PAR PAIEMENT ===
    -- On utilise directement l'ID standard (1=Credit Card, 2=Cash)
    COUNT(CASE WHEN payment_type_id = 1 THEN 1 END) AS credit_card_trips,
    COUNT(CASE WHEN payment_type_id = 2 THEN 1 END) AS cash_trips,
    -- PAR PÉRIODE 
    COUNT(CASE WHEN time_of_day = 'Morning' THEN 1 END) AS morning_trips,
    COUNT(CASE WHEN time_of_day = 'Afternoon' THEN 1 END) AS afternoon_trips,
    COUNT(CASE WHEN time_of_day = 'Evening' THEN 1 END) AS evening_trips,
    COUNT(CASE WHEN time_of_day = 'Night' THEN 1 END) AS night_trips,
    -- === RUSH HOUR  ===
    -- Définition stricte : Heure de Pointe (Dim_Time) ET Semaine (Dim_Date)
    COUNT(CASE 
        WHEN traffic_peak_period IN ('Morning Peak', 'Evening Peak') 
             AND day_type = 'Weekday' 
        THEN 1 
    END) AS rush_hour_trips,
    -- ANALYSE AÉROPORT
    COUNT(CASE WHEN airport_pickup_flag = 'Pickup at LGA/JFK' THEN 1 END) AS airport_trips,
    -- Montant collecté spécifiquement pour les frais d'aéroport
    ROUND(SUM(CASE WHEN airport_pickup_flag = 'Pickup at LGA/JFK' THEN airport_fee ELSE 0 END)::NUMERIC, 2) AS airport_fees_collected

FROM daily_data_joined

GROUP BY 
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_day_of_week_name,
    day_type

ORDER BY 
    pickup_date DESC