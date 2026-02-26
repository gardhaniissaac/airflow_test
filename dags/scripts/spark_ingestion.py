from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os
from .schema_loader import load_config, load_schema, generate_create_table_sql, generate_deduplicated_table_sql
from .db import execute_query


def get_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("airflow_test")
        .config("spark.jars", "/opt/spark-jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )


def get_dest_config():
    return {
        "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_NAME', 'airflow')}",
        "user": os.getenv("POSTGRES_USER", "airflow"),
        "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
        "driver": "org.postgresql.Driver",
    }


def ingest_csv(table_name, file_path, config_path):
    spark = get_spark()
    config = load_config(config_path)
    schema = load_schema(config_path)

    print(schema)

    df = spark.read.option("header", True).csv(file_path, schema)

    df = df.withColumn("_timestamp", current_timestamp())
    df.show()

    create_table_query = generate_create_table_sql(config=config, is_append_only=True)
    execute_query(create_table_query)

    print("Writing to the destination")
    df.write \
        .format("jdbc") \
        .options(**get_dest_config()) \
        .option("dbtable", table_name) \
        .mode("append") \
        .save()

    spark.stop()


def ingest_json(table_name, file_path, config_path):
    spark = get_spark()
    config = load_config(config_path)
    schema = load_schema(config_path)

    df = spark.read.json(path=file_path, schema=schema, multiLine= True)

    df = df.withColumn("_timestamp", current_timestamp())
    df.show()

    create_table_query = generate_create_table_sql(config=config, is_append_only=True)
    execute_query(create_table_query)

    print("Writing to the destination")
    df.write \
        .format("jdbc") \
        .options(**get_dest_config()) \
        .option("dbtable", table_name) \
        .mode("append") \
        .save()

    spark.stop()

def ingest_deduplication(table_name, config_path):
    spark = get_spark()
    config = load_config(config_path)

    query = generate_deduplicated_table_sql(config=config)

    df = spark.read \
        .format("jdbc") \
        .options(**get_dest_config()) \
        .option("query", query) \
        .load()
    
    df.show()

    create_table_query = generate_create_table_sql(config=config, is_append_only=False)
    execute_query(create_table_query)

    print("Writing to the destination")
    df.write \
        .format("jdbc") \
        .options(**get_dest_config()) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()

    spark.stop()


def ingest_postgres(table_name, query, config_path):
    spark = get_spark()
    config = load_config(config_path)

    df = spark.read \
        .format("jdbc") \
        .options(**get_dest_config()) \
        .option("query", query) \
        .load()
    
    df.show()

    create_table_query = generate_create_table_sql(config=config, is_append_only=False)
    execute_query(create_table_query)

    print("Writing to the destination")
    df.write \
        .format("jdbc") \
        .options(**get_dest_config()) \
        .option("dbtable", table_name) \
        .mode("append") \
        .save()

    spark.stop()