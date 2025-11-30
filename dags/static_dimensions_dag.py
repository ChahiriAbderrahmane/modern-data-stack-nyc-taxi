from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pathlib import Path
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.operators import DbtTestOperator
from cosmos.constants import TestBehavior

# Configuration
DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/nyc_yellow_taxi_dwh")
DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"

# Profil de connexion
profile_config = ProfileConfig(
    profile_name="nyc_yellow_taxi_dwh",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",
        profile_args={"schema": "gold"} ))

execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)

with DAG(
    dag_id="build_static_dimensions_dag", 
    start_date=datetime(2025, 11, 30),
    schedule_interval="@once", #
    catchup=False,
    tags=["gold", "static", "dimensions"]
) as dag:

    start = EmptyOperator(task_id="start")

    build_dimensions = DbtTaskGroup(
        group_id="build_dimensions",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["dim_date",  "dim_location", "dim_payment_type", "dim_rate_code", "dim_time", "dim_vendor",  "dim_trip_category"],
            test_behavior=TestBehavior.NONE ),
        operator_args={"install_deps": False}
    )

    end = EmptyOperator(task_id="end")

    start >> build_dimensions >> end