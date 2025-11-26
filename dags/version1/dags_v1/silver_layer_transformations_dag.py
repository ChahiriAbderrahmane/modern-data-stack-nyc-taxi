from datetime import datetime
from pathlib import Path
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/nyc_yellow_taxi_dwh")

profile_config = ProfileConfig(
    profile_name="nyc_yellow_taxi_dwh",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",
        profile_args={"schema": "silver"},
    ),
)

with DAG(
    dag_id="dbt_yellow_taxi_intermediate",
    start_date=datetime(2025, 10, 31),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "silver", "intermediate"],
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
) as dag:
    
    dbt_tg = DbtTaskGroup(
        group_id="transform_yellow_taxi",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["intermediate.yellow_tripdata_2024"]
        ),
        operator_args={
            "install_deps": True,
        },
    )