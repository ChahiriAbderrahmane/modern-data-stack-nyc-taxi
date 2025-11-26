{{
    config(
        materialized='view',
        schema='gold'
    )
}}

-- Rapport de qualité des données
WITH all_records AS (
    SELECT * FROM {{ ref('nyc_tripdata_2024_v2') }}
)

SELECT
    pickup_date,
    pickup_year,
    pickup_month,
    
    -- Volume total
    COUNT(*) AS total_records,
    
    -- Qualité générale
    COUNT(CASE WHEN data_quality_flag = 'Valid' THEN 1 END) AS valid_records,
    COUNT(CASE WHEN data_quality_flag != 'Valid' THEN 1 END) AS invalid_records,
    ROUND(COUNT(CASE WHEN data_quality_flag = 'Valid' THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) AS valid_percentage,
    
    -- Types d'anomalies
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Negative duration' THEN 1 END) AS negative_duration_count,
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Zero duration' THEN 1 END) AS zero_duration_count,
    COUNT(CASE WHEN data_quality_flag = 'Suspicious - Over 24h' THEN 1 END) AS over_24h_count,
    COUNT(CASE WHEN data_quality_flag = 'Suspicious - No distance' THEN 1 END) AS no_distance_count,
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Passenger count' THEN 1 END) AS invalid_passenger_count,
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Negative fare' THEN 1 END) AS negative_fare_count,
    COUNT(CASE WHEN data_quality_flag = 'Invalid - Negative total' THEN 1 END) AS negative_total_count,
    
    -- Passengers
    COUNT(CASE WHEN passenger_count_quality = 'Missing' THEN 1 END) AS missing_passenger_count,
    COUNT(CASE WHEN passenger_count_quality = 'Invalid - Zero passengers' THEN 1 END) AS zero_passengers_count,
    COUNT(CASE WHEN passenger_count_quality = 'Suspicious - Too many' THEN 1 END) AS too_many_passengers_count,
    
    -- Invalid trips
    COUNT(CASE WHEN is_invalid_trip = TRUE THEN 1 END) AS invalid_trip_timing_count,
    
    -- Géographie
    COUNT(CASE WHEN pickup_borough IS NULL THEN 1 END) AS missing_pickup_borough_count,
    COUNT(CASE WHEN dropoff_borough IS NULL THEN 1 END) AS missing_dropoff_borough_count,
    
    -- Impact financier des anomalies
    ROUND(SUM(CASE WHEN data_quality_flag != 'Valid' THEN total_amount_usd ELSE 0 END)::NUMERIC, 2) AS invalid_records_revenue_loss

FROM all_records
GROUP BY 
    pickup_date,
    pickup_year,
    pickup_month
ORDER BY 
    pickup_year DESC,
    pickup_month DESC,
    pickup_date DESC