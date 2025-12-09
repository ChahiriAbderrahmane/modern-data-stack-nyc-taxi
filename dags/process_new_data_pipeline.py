from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from cosmos.operators import DbtTestOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior
from datetime import datetime
from pathlib import Path
import os

# for dbt configurations
DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/nyc_yellow_taxi_dwh")
DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"
CSV_DIR = "/usr/local/airflow/dags/nyc_yellow_taxi_dwh/seeds/second_version/new_arriving_taxi_trips_data"

# Profiles configuration for silver and gold layers
profile_config_silver = ProfileConfig(
    profile_name="nyc_yellow_taxi_dwh",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",
        profile_args={"schema": "silver"}))

profile_config_gold = ProfileConfig(
    profile_name="nyc_yellow_taxi_dwh",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",  
        profile_args={"schema": "gold"}))

# Configuration de l'exécution
execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)

# functions
def detect_new_file():

    current_files = [file_name for file_name in os.listdir(CSV_DIR) if file_name.endswith('.csv')]
    registered_files = Variable.get("registred_files_v2", default_var=[], deserialize_json=True)
    new_files = [f for f in current_files if f not in registered_files]

    if new_files:
        print(f"Nouveaux fichiers détectés : {new_files}")
        return new_files
    return []

def load_new_files_to_postgres(**context):
    new_files = context['ti'].xcom_pull(task_ids='detect_new_file')

    if not new_files:
        print("No new file to be uploaded.")
        return
    
    # connect to Postgres
    hook = PostgresHook(postgres_conn_id='postgres_dbt')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        processed_files = []
        for file_name in new_files:
            csv_path = os.path.join(CSV_DIR, file_name)
        
            cursor.execute("DROP TABLE IF EXISTS temp_table_trips;")
            cursor.execute("""
                CREATE TEMP TABLE temp_table_trips (
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
                    Airport_fee FLOAT
                );
                """)
            
            # copy csv data into the temp table
            with open(csv_path, 'r') as f:
                cursor.copy_expert("""
                    COPY temp_table_trips 
                    FROM STDIN 
                    WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
                """, f)

            # insert into bronze table with the metadata columns 
            cursor.execute(f"""
                INSERT INTO bronze.bronze_taxi_trips (
                    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
                        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee,
                        _source_filename,_ingestion_timestamp)
                SELECT 
                    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
                        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee,
                        '{file_name}',Now()
                FROM temp_table_trips;
            """)
            print(f"the file: {file_name} is inserted in bronze.bronze_taxi_trips.")

            conn.commit()
            processed_files.append(file_name)
        
        registered_files = Variable.get("registred_files_v2", default_var=[], deserialize_json=True)
        all_files = registered_files + processed_files
        Variable.set("registred_files_v2", all_files, serialize_json=True)
        
    except Exception as e:
        conn.rollback()
        print(f'Something went wrong boddy! : {e}')
        raise e
    finally:
        cursor.close()
        conn.close()

###############################    ###############################     ###############################     ###############################     ############################### 

with DAG(
    dag_id="insert_and_process_new_taxi_trips_data",
    start_date=datetime(2025, 11, 23),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["bronze", "silver", "gold", "new_data", "full_pipeline"]
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    detect_new_file_task = ShortCircuitOperator(
        task_id = "detect_new_file",
        python_callable = detect_new_file
    )

    load_new_files_task  = PythonOperator(
        task_id = "load_new_files_to_postgres_in_the_bronze_layer",
        python_callable = load_new_files_to_postgres
    )

    silver_transformations_task = DbtTaskGroup(
        group_id="silver_layer_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config_silver,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["intermediate.nyc_tripdata_2024_v2"],
            test_behavior=TestBehavior.AFTER_EACH, # tester immédiatement après le run
        ),
        operator_args={"install_deps": True},
    )

    build_facts = DbtTaskGroup(
        group_id="build_facts",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config_gold,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["fact_taxi_trips_v2"],
            test_behavior=TestBehavior.NONE),
        operator_args={
            "install_deps": False,}
    )

    build_aggregates = DbtTaskGroup(
        group_id="build_aggregates",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config_gold,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:aggregate"],
            test_behavior=TestBehavior.NONE),
        operator_args={"install_deps": False},
    )

    test_gold_task = DbtTestOperator(
        task_id="test_gold_models",
        project_dir=DBT_PROJECT_PATH,
        profile_config=profile_config_gold,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        select=["marts"],
        install_deps=False
    )

    end = EmptyOperator(task_id="end")

# Order of tasks exection 
    start >> detect_new_file_task >> load_new_files_task >> silver_transformations_task >> build_facts >> build_aggregates >>  test_gold_task >> end 