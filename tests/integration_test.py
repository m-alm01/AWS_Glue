import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

# --------------------------
# Fixtures (Test Setup)
# --------------------------
@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("GlueIntegrationTest") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def test_data_dir(tmp_path):
    """Creates temporary input/output directories for test data"""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return {"input": str(input_dir), "output": str(output_dir)}

# --------------------------
# Transformation Functions
# --------------------------
def clean_phone(phone):
    if phone is None:
        return ""
    digits = ''.join(filter(str.isdigit, str(phone)))
    if digits.startswith('1') and len(digits) == 11:
        digits = digits[1:]
    return digits

def apply_phone_cleaning(spark, input_df):
    """Equivalent to MyTransform using standard PySpark"""
    clean_phone_udf = udf(clean_phone, StringType())
    return input_df.withColumn("Phone 1", clean_phone_udf("Phone 1")) \
                  .withColumn("Phone 2", clean_phone_udf("Phone 2"))

# --------------------------
# Integration Tests
# --------------------------
@pytest.mark.integration
def test_full_workflow(spark_session, test_data_dir):
    """End-to-end test using local filesystem"""
    # 1. Setup test data
    input_path = os.path.join(test_data_dir["input"], "input.csv")
    with open(input_path, "w") as f:
        f.write("Index,Phone 1,Phone 2\n1,(123) 456-7890,+1-987-654-3210\n2,555.123.4567,null")
    
    # 2. Read data
    input_df = spark_session.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(input_path)

    # 3. Apply transformation
    transformed_df = apply_phone_cleaning(spark_session, input_df)

    # 4. Verify transformation
    rows = transformed_df.collect()
    assert rows[0]["Phone 1"] == "1234567890"
    assert rows[0]["Phone 2"] == "9876543210"  # +1 removed
    assert rows[1]["Phone 1"] == "5551234567"
    assert rows[1]["Phone 2"] == ""  # null handled

    # 5. Write output
    output_path = os.path.join(test_data_dir["output"], "output")
    transformed_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    # 6. Verify output was written
    assert os.path.exists(output_path)
    assert len(os.listdir(output_path)) > 0

@pytest.mark.integration
def test_malformed_data(spark_session, test_data_dir):
    """Test handling of invalid phone numbers"""
    input_path = os.path.join(test_data_dir["input"], "bad_data.csv")
    with open(input_path, "w") as f:
        f.write("Phone 1,Phone 2\ninvalid,(123")
    
    # Create schema since we can't infer from malformed data
    schema = StructType([
        StructField("Phone 1", StringType(), True),
        StructField("Phone 2", StringType(), True)
    ])

    input_df = spark_session.read \
        .option("header", True) \
        .schema(schema) \
        .csv(input_path)

    transformed_df = apply_phone_cleaning(spark_session, input_df)
    rows = transformed_df.collect()

    assert rows[0]["Phone 1"] == ""  # invalid -> empty string
    assert rows[0]["Phone 2"] == "123"  # partial number kept

@pytest.mark.integration
def test_empty_input(spark_session, test_data_dir):
    """Test handling of empty input file"""
    input_path = os.path.join(test_data_dir["input"], "empty.csv")
    with open(input_path, "w") as f:
        f.write("Phone 1,Phone 2")  # header only
    
    # Need schema for empty file
    schema = StructType([
        StructField("Phone 1", StringType(), True),
        StructField("Phone 2", StringType(), True)
    ])

    input_df = spark_session.read \
        .option("header", True) \
        .schema(schema) \
        .csv(input_path)

    transformed_df = apply_phone_cleaning(spark_session, input_df)
    assert transformed_df.count() == 0

# --------------------------
# Unit Tests for UDF
# --------------------------
@pytest.mark.parametrize("input_phone,expected", [
    ("(123) 456-7890", "1234567890"),
    ("+1-800-555-1234", "8005551234"),
    ("123", "123"),
    ("", ""),
    (None, ""),
    ("invalid", ""),
    ("1-800-555-1234", "8005551234"),  # US number with leading 1
    ("+44 20 7946 0958", "442079460958")  # UK number
])
def test_phone_cleaning_udf(input_phone, expected):
    assert clean_phone(input_phone) == expected
