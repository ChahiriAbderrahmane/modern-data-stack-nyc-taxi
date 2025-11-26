with src as (
    select * from {{ source('raw_data', 'yellow_tripdata_2024_01') }}
    UNION all 
    select * from {{ source('raw_data', 'yellow_tripdata_2024_02') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_03') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_04') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_05') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_06') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_07') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_08') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_09') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_10') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_11') }}
    UNION all
    select * from {{ source('raw_data', 'yellow_tripdata_2024_12') }}
),
base_data AS (
 select distinct vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    ROUND((EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60),2)::FLOAT AS trip_duration_minutes
from src)
SELECT vendorid, -- je vais laisser les ligne où ça ne correspond pas à 2,1,... à 45 par ex pour tester la performance des résultats requis dans le rapport
case when vendorid = 1 then 'Creative Mobile Technologies, LLC'
 when vendorid  = 2 then 'Curb Mobility, LLC'
 when vendorid = 6 then 'Myle Technologies Inc'
 when vendorid = 7 then 'Helix' 
ELSE 'Other' end as vendor_name,
tpep_pickup_datetime, 
EXTRACT(YEAR FROM tpep_pickup_datetime)  AS pickup_year,
EXTRACT(MONTH FROM tpep_pickup_datetime) AS pickup_month,
EXTRACT(DAY FROM tpep_pickup_datetime)   AS pickup_day,
EXTRACT(HOUR FROM tpep_pickup_datetime)  AS pickup_hour,
EXTRACT(MINUTE FROM tpep_pickup_datetime)  AS pickup_minute,
EXTRACT(SECOND FROM tpep_pickup_datetime)  AS pickup_second,
tpep_dropoff_datetime, 
EXTRACT(YEAR FROM tpep_dropoff_datetime)  AS dropoff_year,
EXTRACT(MONTH FROM tpep_dropoff_datetime) AS dropoff_month,
EXTRACT(DAY FROM tpep_dropoff_datetime)   AS dropoff_day,
EXTRACT(HOUR FROM tpep_dropoff_datetime)  AS dropoff_hour,
EXTRACT(MINUTE FROM tpep_dropoff_datetime)  AS dropoff_minute,
EXTRACT(SECOND FROM tpep_dropoff_datetime)  AS dropoff_second,
COALESCE(passenger_count, 0) as passenger_count_that_day,
CASE 
    WHEN passenger_count IS NULL THEN 'Missing'
    WHEN passenger_count = 0 THEN 'Invalid - Zero passengers'
    WHEN passenger_count > 6 THEN 'Suspicious - Too many'
    ELSE 'Valid'
END AS passenger_count_quality,
ROUND((EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60),2)::FLOAT AS trip_duration_minutes,
trip_distance, 
ratecodeid,
COALESCE(RatecodeID, 99) AS RatecodeID_clean,
CASE
    WHEN RatecodeID = 1 THEN 'Standard rate'
    WHEN RatecodeID = 2 THEN 'JFK'
    WHEN RatecodeID = 3 THEN 'Newark'
    WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
    WHEN RatecodeID = 5 THEN 'Negotiated fare'
    WHEN RatecodeID = 6 THEN 'Group ride'
    WHEN RatecodeID = 99 OR RatecodeID IS NULL THEN 'Unknown'
    ELSE 'Other'
END AS rate_code_description,
store_and_fwd_flag,
COALESCE(store_and_fwd_flag, 'N') AS store_and_fwd_flag_cleaned,
CASE
    WHEN store_and_fwd_flag = 'Y' THEN 'Store and forward trip'
    WHEN store_and_fwd_flag = 'N' THEN 'Not a store and forward trip'
    WHEN store_and_fwd_flag IS NULL THEN 'Unknown'
    ELSE 'Invalid flag'
END AS store_and_fwd_description,
pulocationid, 
pu_zone.borough AS pickup_borough,
pu_zone.zone AS pickup_zone,
dolocationid,
do_zone.borough AS dropoff_borough,
do_zone.zone AS dropoff_zone,
payment_type, 
CASE payment_type
    WHEN 0 THEN 'Flex Fare trip'
    WHEN 1 THEN 'Credit card'
    WHEN 2 THEN 'Cash'
    WHEN 3 THEN 'No charge'
    WHEN 4 THEN 'Dispute'
    WHEN 5 THEN 'Unknown'
    WHEN 6 THEN 'Voided trip'
    ELSE 'Other'
END AS payment_type_description,
fare_amount as base_fare_usd, 
extra as surcharges_usd,
mta_tax AS mta_tax_usd, 
tip_amount as tip_amount_usd, 
tolls_amount as tolls_amount_usd, 
improvement_surcharge,
total_amount as total_amount_usd, 
COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
COALESCE(airport_fee, 0) AS airport_fee,
CASE 
    WHEN airport_fee > 0 THEN 'Pickup at LGA/JFK'
    ELSE 'Other location'
END AS airport_pickup_flag,
-- Détecter s'il y'a des anomalies
CASE 
    WHEN trip_duration_minutes < 0 THEN 'Invalid - Negative duration'
    WHEN trip_duration_minutes = 0 THEN 'Invalid - Zero duration'
    WHEN trip_duration_minutes > 1440 THEN 'Suspicious - Over 24h'
    WHEN trip_distance = 0 AND trip_duration_minutes > 5 THEN 'Suspicious - No distance'
    WHEN passenger_count = 0 OR passenger_count > 6 THEN 'Invalid - Passenger count'
    WHEN fare_amount < 0 THEN 'Invalid - Negative fare'
    WHEN total_amount < 0 THEN 'Invalid - Negative total'
    ELSE 'Valid'
END AS data_quality_flag,
	-- Flag pour détecter les courses potentiellement invalides
CASE 
    WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN TRUE
    ELSE FALSE
END AS is_invalid_trip,
-- Vitesse moyenne
CASE 
    WHEN trip_duration_minutes > 0 THEN ROUND((trip_distance / (trip_duration_minutes / 60.0))::NUMERIC, 2)
    ELSE NULL
END AS avg_speed_mph,
-- Tarif par mile/minute
CASE 
    WHEN trip_distance > 0 THEN ROUND((fare_amount / trip_distance)::NUMERIC, 2)
    ELSE NULL
END AS fare_per_mile,
CASE 
    WHEN trip_duration_minutes > 0 THEN ROUND((fare_amount / trip_duration_minutes)::NUMERIC, 2)
    ELSE NULL
END AS fare_per_minute,
-- Pourcentage de pourboire
CASE 
    WHEN fare_amount > 0 THEN ROUND((tip_amount / fare_amount * 100)::NUMERIC, 2)
    ELSE 0
END AS tip_percentage,
-- Revenus totaux (sans les taxes)
(fare_amount + tip_amount + tolls_amount) AS revenue_amount,
-- Jour de la semaine
EXTRACT(DOW FROM tpep_pickup_datetime) AS pickup_day_of_week_num,
TO_CHAR(tpep_pickup_datetime, 'Day') AS pickup_day_of_week_name,
-- Semaine de l'année
EXTRACT(WEEK FROM tpep_pickup_datetime) AS pickup_week_of_year,
-- Trimestre
EXTRACT(QUARTER FROM tpep_pickup_datetime) AS pickup_quarter,
-- Période de la journée
CASE 
    WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 18 AND 22 THEN 'Evening'
    ELSE 'Night'
END AS time_of_day,
-- Week-end vs semaine
CASE 
    WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN 'Weekend'
    ELSE 'Weekday'
END AS day_type,
-- Heure de pointe
CASE 
    WHEN EXTRACT(DOW FROM tpep_pickup_datetime) BETWEEN 1 AND 5 
         AND (EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 7 AND 9 
              OR EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 17 AND 19)
    THEN 'Rush Hour'
    ELSE 'Off-Peak'
END AS rush_hour_flag,
-- Distance
CASE 
    WHEN trip_distance < 1 THEN 'Short (< 1 mile)'
    WHEN trip_distance BETWEEN 1 AND 5 THEN 'Medium (1-5 miles)'
    WHEN trip_distance BETWEEN 5 AND 10 THEN 'Long (5-10 miles)'
    WHEN trip_distance > 10 THEN 'Very Long (> 10 miles)'
    ELSE 'Unknown'
END AS distance_category,
-- Durée
CASE 
    WHEN trip_duration_minutes < 5 THEN 'Very Short (< 5 min)'
    WHEN trip_duration_minutes BETWEEN 5 AND 15 THEN 'Short (5-15 min)'
    WHEN trip_duration_minutes BETWEEN 15 AND 30 THEN 'Medium (15-30 min)'
    WHEN trip_duration_minutes BETWEEN 30 AND 60 THEN 'Long (30-60 min)'
    WHEN trip_duration_minutes > 60 THEN 'Very Long (> 60 min)'
    ELSE 'Invalid'
END AS duration_category,
-- Montant
CASE 
    WHEN total_amount < 10 THEN 'Low (< $10)'
    WHEN total_amount BETWEEN 10 AND 25 THEN 'Medium ($10-$25)'
    WHEN total_amount BETWEEN 25 AND 50 THEN 'High ($25-$50)'
    WHEN total_amount > 50 THEN 'Very High (> $50)'
    ELSE 'Invalid'
END AS fare_category,
-- Date complète pour faciliter les GROUP BY
DATE(tpep_pickup_datetime) AS pickup_date,
DATE(tpep_dropoff_datetime) AS dropoff_date
FROM base_data bd
LEFT JOIN bronze.taxi_zone_lookup_data pu_zone
    ON bd.PULocationID = pu_zone.LocationID
LEFT JOIN bronze.taxi_zone_lookup_data do_zone
    ON bd.DOLocationID = do_zone.LocationID
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024