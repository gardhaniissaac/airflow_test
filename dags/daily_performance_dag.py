from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.spark_ingestion import ingest_postgres

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="daily_performances_dag",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    
    query = """
        WITH active_students_by_class AS (
            SELECT
                class_id,
                COUNT(DISTINCT student_id) AS total_students
            FROM students
            WHERE enrollment_status = 'ACTIVE'
            GROUP BY class_id
        ),

        attendance_daily AS (
            SELECT
                s.class_id,
                DATE(a.attendance_date) AS date,
                COUNT(DISTINCT a.student_id) AS students_with_attendance,
                SUM(CASE WHEN LOWER(a.status) = 'present' THEN 1 ELSE 0 END) AS present_count,
                SUM(CASE WHEN LOWER(a.status) = 'absent' THEN 1 ELSE 0 END) AS absent_count
            FROM attendances a
            JOIN students s
                ON a.student_id = s.student_id
            GROUP BY s.class_id, DATE(a.attendance_date)
        ),

        assessment_daily AS (
            SELECT
                s.class_id,
                DATE(assessment_date) AS date,
                COUNT(DISTINCT a.student_id) AS students_with_assessment,
                COUNT(a.assessment_id) AS assessment_count,
                AVG(CAST(a.score AS DECIMAL)) AS avg_score
            FROM assessments a
            JOIN students s
                ON a.student_id = s.student_id
            GROUP BY s.class_id, DATE(assessment_date)
        )

        SELECT
            ad.class_id,
            ad.date,
            ast.total_students,
            ad.students_with_attendance,
            ad.present_count,
            ad.absent_count,
            ROUND(
                ad.present_count * 1.0 / NULLIF(ast.total_students, 0),
                2
            ) AS attendance_rate,
            COALESCE(asd.students_with_assessment, 0) AS students_with_assessment,
            COALESCE(asd.assessment_count, 0) AS assessment_count,
            ROUND(COALESCE(asd.avg_score, 0), 2) AS avg_score

        FROM attendance_daily ad
        JOIN active_students_by_class ast
            ON ad.class_id = ast.class_id
        LEFT JOIN assessment_daily asd
            ON ad.class_id = asd.class_id
            AND ad.date = asd.date
    """

    ingest_pg_task = PythonOperator(
        task_id="ingest_postgres",
        python_callable=ingest_postgres,
        op_kwargs={
            "table_name": "daily_performances",
            "query": query,
            "config_path": "/opt/airflow/dags/configs/daily_performances.yaml",
        },
    )

    ingest_pg_task