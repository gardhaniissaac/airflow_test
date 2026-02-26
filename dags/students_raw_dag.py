from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.spark_ingestion import ingest_csv

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="students_raw_dag",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_csv_task = PythonOperator(
        task_id="ingest_csv",
        python_callable=ingest_csv,
        op_kwargs={
            "table_name": "students_raw",
            "file_path": "/opt/airflow/dags/resources/students.csv",
            "config_path": "/opt/airflow/dags/configs/students_raw.yaml",
        },
    )

    # ingest_pg_task = PythonOperator(
    #     task_id="ingest_postgres",
    #     python_callable=ingest_postgres,
    #     op_kwargs={
    #         "query": "SELECT id, name, email, age FROM source_table",
    #         "schema_path": "/opt/airflow/dags/configs/sample_schema.yaml",
    #     },
    # )

    ingest_csv_task