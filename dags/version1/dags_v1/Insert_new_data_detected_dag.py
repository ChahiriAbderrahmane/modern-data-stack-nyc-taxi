from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json

csv_dir = "/usr/local/airflow/dags/nyc_yellow_taxi_dwh/seeds"
register_file_path = '/usr/local/airflow/dags/register_files.json'

def detect_new_file():

    current_files = [file_name for file_name in os.listdir(csv_dir) if file_name.endswith('.csv')]
    registered_files = Variable.get("registred_files", default_var=[], deserialize_json=True)
    new_files = [f for f in current_files if f not in registered_files]

    if new_files:
        all_files = registered_files + new_files
        Variable.set("registred_files", all_files, serialize_json=True)
        print(f"Nouveaux fichiers détectés : {new_files}")
        return new_files
    return []

def load_new_files_to_postgres(**context):

    new_files = context['ti'].xcom_pull(task_ids='detect_new_file')

    if new_files == []:
        print("Aucun nouveau fichier à charger.")
        
    else:
        # connect to Postgres
        hook = PostgresHook(postgres_conn_id='postgres_dbt')
        conn = hook.get_conn()
        cursor = conn.cursor()

        for file_name in new_files:
            table_name = file_name.replace('.csv','').replace('-','_')
            csv_path = os.path.join(csv_dir, file_name)

            # DROP + CREATE table
            cursor.execute(f"""
                ALTER TABLE bronze.{table_name} (
                    LocationID INTEGER,
                    Borough VarCHAR(50),
                    Zone VarCHAR(50),
                    service_zone VarCHAR(50)
                );
            """)

            # COPY ultra-rapide
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
    dag_id = "detect_new_arrived_data_dag",
    start_date = datetime(2025,10,30),
    schedule_interval = "*/5 * * * *",
    catchup = False,
    tags= ["auto_load", "sensor", "bronze_layer"],
) as dag:
    
    detect_task = PythonOperator(
        task_id = "detect_new_file",
        python_callable = detect_new_file,
    )

    load_task  = PythonOperator(
        task_id = "load_new_files_to_postgres",
        python_callable = load_new_files_to_postgres,
    )

    detect_task >> load_task