from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.spark_ingestion import ingest_csv

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="attendances_raw_dag",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_csv_task = PythonOperator(
        task_id="ingest_csv",
        python_callable=ingest_csv,
        op_kwargs={
            "table_name": "attendances_raw",
            "file_path": "/opt/airflow/dags/resources/attendances.csv",
            "config_path": "/opt/airflow/dags/configs/attendances_raw.yaml",
        },
    )

    ingest_csv_task