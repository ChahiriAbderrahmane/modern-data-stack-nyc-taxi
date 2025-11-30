{{ config( materialized='view', schema='gold') }}

-- Rapport de qualité des données (Data Observability)
-- Source : SILVER (car la Gold Fact Table a déjà filtré les mauvaises données)
WITH silver_source AS (
    SELECT * FROM {{ ref('nyc_tripdata_2024_v2') }}
),

silver_source_more AS (
    SELECT 
        s.*,
        p_zone.borough as pickup_borough,
        d_zone.borough as dropoff_borough
    FROM silver_source s
    -- On utilise LEFT JOIN pour détecter les IDs qui ne matchent rien (NULL)
    LEFT JOIN {{ ref('dim_location') }} p_zone ON s.pulocationid = p_zone.locationid
    LEFT JOIN {{ ref('dim_location') }} d_zone ON s.dolocationid = d_zone.locationid
)

SELECT
    pickup_date,
    EXTRACT(YEAR FROM pickup_date) AS pickup_year,
    EXTRACT(MONTH FROM pickup_date) AS pickup_month,
    -- VOLUME & SANTÉ GLOBALE
    COUNT(*) AS total_records,
    -- Validité
    COUNT(CASE WHEN data_quality_flag = 'Valid' THEN 1 END) AS valid_records,
    COUNT(CASE WHEN data_quality_flag != 'Valid' THEN 1 END) AS invalid_records,
    -- Score de Qualité (%) - KPI critique
    ROUND(
        COUNT(CASE WHEN data_quality_flag = 'Valid' THEN 1 END)::NUMERIC 
        / NULLIF(COUNT(*), 0)::NUMERIC * 100
    , 2) AS quality_score_pct,
    -- DÉTAIL DES ANOMALIES CRITIQUES (Bloquantes)
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Negative duration' THEN 1 END) AS error_negative_duration,
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Zero duration' THEN 1 END) AS error_zero_duration,
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Passenger count' THEN 1 END) AS error_invalid_passengers,
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Negative fare' THEN 1 END) AS error_negative_fare,
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Negative total' THEN 1 END) AS error_negative_total,
    -- DÉTAIL DES ANOMALIES SUSPECTES (Warnings)
    COUNT(CASE WHEN data_quality_flag = 'Suspicious - Over 24h' THEN 1 END) AS warn_duration_over_24h,
    COUNT(CASE WHEN data_quality_flag = 'Suspicious - No distance' THEN 1 END) AS warn_no_distance,
    -- QUALITÉ PASSAGERS (Focus spécifique)
    COUNT(CASE WHEN passenger_count_quality = 'Missing' THEN 1 END) AS pass_missing,
    COUNT(CASE WHEN passenger_count_quality = 'Invalid - Zero passengers' THEN 1 END) AS pass_zero,
    COUNT(CASE WHEN passenger_count_quality = 'Suspicious - Too many' THEN 1 END) AS pass_too_many,
    -- COHÉRENCE TEMPORELLE
    -- Dropoff avant Pickup ?
    COUNT(CASE WHEN is_invalid_trip = TRUE THEN 1 END) AS error_time_travel,
    -- COMPLÉTITUDE GÉOGRAPHIQUE
    -- Si pickup_borough est NULL, c'est que l'ID n'est pas dans le fichier Zone Lookup
    COUNT(CASE WHEN pickup_borough IS NULL OR pickup_borough = 'Unknown' THEN 1 END) AS missing_pickup_location,
    COUNT(CASE WHEN dropoff_borough IS NULL OR dropoff_borough = 'Unknown' THEN 1 END) AS missing_dropoff_location,
    -- IMPACT FINANCIER (Revenue at Risk)
    -- Combien d'argent représentent ces lignes "sales" ?
    -- C'est l'argument n°1 pour demander du budget pour nettoyer les données sources
    ROUND(
        SUM(CASE WHEN data_quality_flag != 'Valid' THEN total_amount_usd ELSE 0 END)::NUMERIC
    , 2) AS revenue_at_risk_usd

FROM silver_source_more

GROUP BY 
    pickup_date,
    EXTRACT(YEAR FROM pickup_date),
    EXTRACT(MONTH FROM pickup_date)

ORDER BY 
    pickup_date DESC