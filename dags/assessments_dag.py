from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.spark_ingestion  import ingest_deduplication

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="assessments_dag",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_pg_task = PythonOperator(
        task_id="ingest_deduplication",
        python_callable=ingest_deduplication,
        op_kwargs={
            "table_name": "assessments",
            "config_path": "/opt/airflow/dags/configs/assessments.yaml",
        },
    )

    ingest_deduplication