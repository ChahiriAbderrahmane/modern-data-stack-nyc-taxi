from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import os


def load_csv_to_postgres(**context):
    hook = PostgresHook(postgres_conn_id='postgres_dbt')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Chemin vers CSVs
    csv_dir = "/usr/local/airflow/dags/nyc_yellow_taxi_dwh/seeds"
    
    # Liste des fichiers CSV
    csv_files = {
        'yellow_tripdata_2024-01.csv': 'yellow_tripdata_2024_01',
        'yellow_tripdata_2024-02.csv': 'yellow_tripdata_2024_02',
        'yellow_tripdata_2024-03.csv': 'yellow_tripdata_2024_03',
        'yellow_tripdata_2024-04.csv': 'yellow_tripdata_2024_04',
        'yellow_tripdata_2024-05.csv': 'yellow_tripdata_2024_05',
        'yellow_tripdata_2024-06.csv': 'yellow_tripdata_2024_06',
        'yellow_tripdata_2024-07.csv': 'yellow_tripdata_2024_07',
        'yellow_tripdata_2024-08.csv': 'yellow_tripdata_2024_08',
        'yellow_tripdata_2024-09.csv': 'yellow_tripdata_2024_09',
        'yellow_tripdata_2024-10.csv': 'yellow_tripdata_2024_10',
        'yellow_tripdata_2024-11.csv': 'yellow_tripdata_2024_11',
        'yellow_tripdata_2024-12.csv': 'yellow_tripdata_2024_12',
    }
    
    for csv_file, table_name in csv_files.items():
        csv_path = os.path.join(csv_dir, csv_file)
        
        if not os.path.exists(csv_path):
            print(f"Fichier non trouvé : {csv_path}")
            continue
        
        # DROP + CREATE table
        cursor.execute(f"""
            DROP TABLE IF EXISTS bronze.{table_name} CASCADE;
            CREATE TABLE bronze.{table_name} (
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
                insertion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # COPY est apparament ultra-rapide
        with open(csv_path, 'r') as f:
            cursor.copy_expert(f"""
                COPY bronze.{table_name} 
                FROM STDIN 
                WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """, f)
        
        conn.commit()
        print(f"Loaded {csv_path} into {table_name}")
    
    cursor.close()
    conn.close()


with DAG(
    dag_id="load_raw_data_to_bronze_layer",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze"]
) as dag:
    
    load_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres
    )