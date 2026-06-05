# steam_daily_pipeline.py - The Airflow DAG (The Manager)
# Orchestrates the Bronze → Silver → Gold pipeline hourly.
# Compaction runs every 3 days to prevent the Spark small-files problem.
#
# All scripts run directly inside the Airflow container
# (custom image has Java + PySpark baked in via Dockerfile.airflow)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def should_compact(**context):
    """
    Run compaction every 3 days.
    Returns True only if today's day-of-year is divisible by 3,
    short-circuiting (skipping) the compact task on all other runs.
    """
    return datetime.utcnow().timetuple().tm_yday % 3 == 0


with DAG(
    dag_id="steam_hourly_pipeline",
    default_args=default_args,
    description="Hourly Steam ETL (Rate-Limit Safe): Bronze (API) → Silver (Spark) → Gold (DB)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "etl", "medallion"],
) as dag:

    # ==================== TASK 0a: Compact Gate ====================
    # Short-circuits the compaction task on non-compaction days,
    # so it has zero overhead on the other 22+ hourly runs each day.
    compact_gate = ShortCircuitOperator(
        task_id="compact_gate",
        python_callable=should_compact,
        ignore_downstream_trigger_rules=False,
    )

    # ==================== TASK 0b: Compact Bronze ====================
    # Merges batch files older than 7 days into monthly archives.
    # Runs every 3 days to prevent the Spark small-files problem.
    compact_bronze = BashOperator(
        task_id="compact_bronze",
        bash_command="cd /project && python scripts/0_compact_bronze.py",
    )

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
    #
    #   compact_gate ──► compact_bronze ──┐
    #                                     ▼
    #                             extract_bronze ──► transform_silver ──► load_gold ──► test_pipeline
    #
    # compact_gate short-circuits compact_bronze on non-compaction days,
    # but extract_bronze runs unconditionally every hour regardless.
    compact_gate >> compact_bronze
    extract_bronze >> transform_silver >> load_gold >> test_pipeline
