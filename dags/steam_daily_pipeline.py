# steam_daily_pipeline.py - The Airflow DAG (The Manager)
# Orchestrates the Bronze → Silver → Gold pipeline daily
#
# All scripts run directly inside the Airflow container
# (custom image has Java + PySpark baked in via Dockerfile.airflow)

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="steam_daily_pipeline",
    default_args=default_args,
    description="Daily Steam ETL: Bronze (API) → Silver (PySpark) → Gold (PostgreSQL)",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["steam", "etl", "medallion"],
) as dag:

    # ==================== TASK 1: Extract Bronze ====================
    # Calls the Steam API and saves raw JSON to data/bronze/
    extract_bronze = BashOperator(
        task_id="extract_bronze",
        bash_command="cd /project && python scripts/1_extract_bronze.py",
    )

    # ==================== TASK 2: Transform Silver ====================
    # PySpark reads bronze JSON, cleans/transforms → 7 CSV tables in data/silver/
    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command="cd /project && python scripts/2_transform_silver.py",
    )

    # ==================== TASK 3: Load Gold ====================
    # PySpark reads silver CSVs, upserts into PostgreSQL gold tables
    load_gold = BashOperator(
        task_id="load_gold",
        bash_command="cd /project && python scripts/3_load_gold.py",
    )

    # ==================== TASK 4: Validate Pipeline ====================
    # Runs integrity tests across all 3 layers (Bronze → Silver → Gold)
    test_pipeline = BashOperator(
        task_id="test_pipeline",
        bash_command="cd /project && python scripts/4_test_pipeline.py",
    )

    # ==================== Pipeline Order ====================
    extract_bronze >> transform_silver >> load_gold >> test_pipeline
