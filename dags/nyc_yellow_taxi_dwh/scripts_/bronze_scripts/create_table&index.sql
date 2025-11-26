CREATE TABLE bronze.bronze_taxi_trips (
				VendorID INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count FLOAT,
                trip_distance FLOAT,
                RatecodeID FLOAT,
                store_and_fwd_flag VARCHAR(1),
                PULocationID INTEGER,
                DOLocationID INTEGER,
                payment_type INTEGER,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT,
                Airport_fee FLOAT,
			    _source_filename TEXT,
			    _ingestion_timestamp TIMESTAMP DEFAULT NOW()
);
-- index pour accélérer dbt, car il va se baser sur ce timestamp pour identifier les nouvelles lignes à traiter
CREATE INDEX idx_bronze_ingestion ON bronze.bronze_taxi_trips (_ingestion_timestamp); 