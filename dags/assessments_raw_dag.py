from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.spark_ingestion  import ingest_json

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="assessments_raw_dag",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_json_task = PythonOperator(
        task_id="ingest_json",
        python_callable=ingest_json,
        op_kwargs={
            "table_name": "assessments_raw",
            "file_path": "/opt/airflow/dags/resources/assessments.json",
            "config_path": "/opt/airflow/dags/configs/assessments_raw.yaml",
        },
    )

    ingest_json_task