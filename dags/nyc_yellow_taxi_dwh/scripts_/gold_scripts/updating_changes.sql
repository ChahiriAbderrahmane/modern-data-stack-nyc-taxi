ALTER TABLE gold.fact_taxi_trips_v2
RENAME COLUMN passenger_count_that_day TO passenger_count_in_trip;

UPDATE gold.fact_taxi_trips_v2
SET passenger_count_quality = 'Missing'
WHERE passenger_count_in_trip = 0;