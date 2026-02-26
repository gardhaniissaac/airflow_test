import yaml
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType

PG_TYPE_MAPPING = {
    "string": "TEXT",
    "integer": "INTEGER",
    "float": "DECIMAL",
    "timestamp": "TIMESTAMP"
}

DF_TYPE_MAPPING = {
    'integer': IntegerType(),
    'string': StringType(),
    'float': FloatType(),
    'timestamp': TimestampType()
}

def load_config(path):
    with open(path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config

def load_schema(path):
    config = load_config(path)
    fields = [StructField(f['name'], DF_TYPE_MAPPING[f['type']], True) for f in config['fields']]
    pyspark_schema = StructType(fields)

    return pyspark_schema

def generate_create_table_sql(config, is_append_only=False):
    table_name = config["table_name"]
    primary_key = config.get("primary_key")
    fields = config["fields"]

    column_defs = []

    for field in fields:
        col_name = field["name"]
        col_type = PG_TYPE_MAPPING[field["type"]]

        if not is_append_only and col_name == primary_key:
            column_defs.append(f"{col_name} {col_type} PRIMARY KEY")
        else:
            column_defs.append(f"{col_name} {col_type}")

    columns_sql = ",\n    ".join(column_defs)

    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    """

def generate_deduplicated_table_sql(config):
    table_name = config["table_name"]
    primary_key = config.get("primary_key")
    fields = config["fields"]

    column_defs = []

    for field in fields:
        column_defs.append(field["name"])

    columns_sql = ",\n    ".join(column_defs)

    return f"""
    WITH ranked_table as (
        SELECT
            {columns_sql},
            RANK() OVER (PARTITION BY {primary_key} ORDER BY _timestamp DESC) as rank_latest
        FROM {table_name}_raw
    )
    SELECT {columns_sql}
    FROM ranked_table
    WHERE rank_latest=1
    """
    