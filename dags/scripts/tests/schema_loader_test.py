import pytest
import tempfile
import yaml
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType

from scripts.schema_loader import (
    load_schema,
    generate_create_table_sql,
    generate_deduplicated_table_sql,
)


@pytest.fixture
def sample_config():
    return {
        "table_name": "students",
        "primary_key": "student_id",
        "fields": [
            {"name": "student_id", "type": "string"},
            {"name": "student_name", "type": "string"},
            {"name": "age", "type": "integer"},
            {"name": "gpa", "type": "float"},
            {"name": "_timestamp", "type": "timestamp"},
        ],
    }


def test_load_schema(sample_config):
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        yaml.dump(sample_config, tmp)
        tmp_path = tmp.name

    schema = load_schema(tmp_path)

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 5
    assert isinstance(schema["student_id"].dataType, StringType)
    assert isinstance(schema["age"].dataType, IntegerType)
    assert isinstance(schema["gpa"].dataType, FloatType)
    assert isinstance(schema["_timestamp"].dataType, TimestampType)


def test_generate_create_table_sql_with_pk(sample_config):
    sql = generate_create_table_sql(sample_config)

    assert "CREATE TABLE IF NOT EXISTS students" in sql
    assert "student_id TEXT PRIMARY KEY" in sql
    assert "student_name TEXT" in sql
    assert "age INTEGER" in sql
    assert "gpa DECIMAL" in sql


def test_generate_create_table_sql_append_only(sample_config):
    sql = generate_create_table_sql(sample_config, is_append_only=True)
    assert "PRIMARY KEY" not in sql


def test_generate_deduplicated_table_sql(sample_config):
    sql = generate_deduplicated_table_sql(sample_config)

    assert "WITH ranked_table as" in sql
    assert "RANK() OVER (PARTITION BY student_id ORDER BY _timestamp DESC)" in sql
    assert "FROM students_raw" in sql
    assert "WHERE rank_latest=1" in sql