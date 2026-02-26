import psycopg2
import os

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "admin"),
        port=os.getenv("POSTGRES_PORT", 5433),
    )


def execute_query(query, values=None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, values or ())
    conn.commit()
    cur.close()
    conn.close()


def fetch_query(query):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(query)

        if cur.description is None:
            return [], []

        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        return colnames, rows

    finally:
        cur.close()
        conn.close()