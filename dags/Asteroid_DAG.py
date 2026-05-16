from __future__ import annotations
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

DAG_ID = "nasa_neo_etl_pipeline"
PYTHON = "python3"

EXTRACT_API = "/opt/airflow/scripts/Extract_API.py"
TRANSFORM_FLATTEN = "/opt/airflow/scripts/transform_flatten.py"
TRANSFORM_CSV = "/opt/airflow/scripts/transform_toCSV.py"
VALIDATE = "/opt/airflow/scripts/validate.py"
LOAD_POSTGRES = "/opt/airflow/scripts/load_postgres.py"
CAL_DANGER_LEVEL = "/opt/airflow/scripts/danger_level.py"

default_args = {
    "owner" : "hieu",
    "depends_on_past" : False,
    "retries" : 3,
    "retry_delay" : timedelta(minutes=1)
}
with DAG(
    dag_id= DAG_ID,
    description= "Extract API -> JSON -> CSV -> Validate CSV-> Postgres -> DW -> Analytics Task using ETL + Airflow",
    default_args= default_args,
    start_date= days_ago(1),
    schedule_interval = "@daily",
    catchup= False,
    tags= ["nasa","etl","asteroids"]
) as dag:
    start = EmptyOperator(task_id = "start")
    
    extract = BashOperator(
        task_id = "extract_API_to_JSON",
        bash_command = f"{PYTHON} {EXTRACT_API} '{{{{ ds }}}}'",
    )

    transform_flatten = BashOperator(
        task_id = "flatten_JSON",
        bash_command = f"{PYTHON} {TRANSFORM_FLATTEN} '{{{{ ds }}}}'",
    )

    transform_csv = BashOperator(
        task_id = "to_CSV",
        bash_command = f"{PYTHON} {TRANSFORM_CSV} '{{{{ ds }}}}'",
    )

    validate_task = BashOperator(
        task_id = "validate_csv_data",
        bash_command = f"{PYTHON} {VALIDATE} '{{{{ ds }}}}'",
    )

    load_to_postgre = BashOperator(
        task_id = "load_CSV_to_DW",
        bash_command = f"{PYTHON} {LOAD_POSTGRES} '{{{{ ds }}}}'",
    )

    danger_level = BashOperator(
        task_id = "calculate_danger_level",
        bash_command = f"{PYTHON} {CAL_DANGER_LEVEL} '{{{{ ds }}}}'",
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> transform_flatten >> transform_csv >> validate_task >> load_to_postgre >> [danger_level] >> end
