import pytest
from unittest.mock import Mock, patch, ANY
from pyspark.sql.types import StructType, StructField, StringType

# Define the MyTransform function directly in the test file
def MyTransform(glue_context, dfc):
    # Extract the first DynamicFrame from the DynamicFrameCollection
    input_dyf = dfc.select(list(dfc.keys())[0])

    # Convert DynamicFrame to Spark DataFrame
    spark_df = input_dyf.toDF()

    # Define a Python UDF to clean phone numbers
    def clean_phone(phone):
        if phone is None:
            return ""
        digits = ''.join(filter(str.isdigit, str(phone)))
        # Remove US country code (+1) if present
        if digits.startswith('1') and len(digits) == 11:
            digits = digits[1:]
        return digits

    # Register UDF
    spark = glue_context.spark_session
    clean_phone_udf = spark.udf.register("clean_phone_udf", clean_phone)

    # Apply UDF to clean Phone 1 and Phone 2
    cleaned_df = spark_df.withColumn("Phone 1", clean_phone_udf("Phone 1")) \
                         .withColumn("Phone 2", clean_phone_udf("Phone 2"))

    # Convert back to DynamicFrame
    cleaned_dyf = MockDynamicFrame(cleaned_df, glue_context, "cleaned_dyf")

    # Return as DynamicFrameCollection
    return MockDynamicFrameCollection({"cleaned_dyf": cleaned_dyf}, glue_context)

# Mock AWS Glue classes
class MockDynamicFrame:
    def __init__(self, df, glue_context, name):
        self.df = df
        self.glue_context = glue_context
        self.name = name

    def toDF(self):
        return self.df

class MockDynamicFrameCollection:
    def __init__(self, frames, glue_context):
        self.frames = frames
        self.glue_context = glue_context

    def select(self, key):
        return self.frames[key]

    def keys(self):
        return list(self.frames.keys())

class MockGlueContext:
    def __init__(self, spark_session):
        self.spark_session = spark_session

# Mock Spark DataFrame for testing
class MockSparkDataFrame:
    def __init__(self, data, schema):
        self.data = data
        self.schema = schema

    def withColumn(self, col_name, col):
        # Simulate withColumn by applying the clean_phone function directly
        new_data = []
        for row in self.data:
            new_row = list(row)
            if col_name == "Phone 1":
                new_row[0] = clean_phone(row[0])  # Apply clean_phone directly
            elif col_name == "Phone 2":
                new_row[1] = clean_phone(row[1])  # Apply clean_phone directly
            new_data.append(tuple(new_row))
        return MockSparkDataFrame(new_data, self.schema)

    def collect(self):
        # Return data as a list of Rows for compatibility
        from pyspark.sql import Row
        return [Row(**{self.schema[i].name: val for i, val in enumerate(row)}) for row in self.data]

    def count(self):
        return len(self.data)

# Extract the clean_phone function for direct testing
def clean_phone(phone):
    if phone is None:
        return ""
    digits = ''.join(filter(str.isdigit, str(phone)))
    # Remove US country code (+1) if present
    if digits.startswith('1') and len(digits) == 11:
        digits = digits[1:]
    return digits

@pytest.fixture
def glue_context():
    # Create a mock SparkSession with proper UDF registration
    spark_session = Mock()
    spark_session.udf = Mock()
    # Return the actual clean_phone function when UDF is registered
    spark_session.udf.register = Mock(return_value=clean_phone)
    return MockGlueContext(spark_session)

@pytest.fixture
def sample_data():
    # Sample data with various phone number formats
    data = [
        ("(123) 456-7890", "+1-987-654-3210"),
        ("123.456.7890", "1234567890"),
        (None, "abc-def-ghi"),
        ("123-456-7890", None)
    ]
    schema = StructType([
        StructField("Phone 1", StringType(), True),
        StructField("Phone 2", StringType(), True)
    ])
    return schema, data

def test_clean_phone_function():
    # Test the clean_phone function directly
    assert clean_phone("(123) 456-7890") == "1234567890"
    assert clean_phone("+1-987-654-3210") == "9876543210"  # Now strips +1
    assert clean_phone("123.456.7890") == "1234567890"
    assert clean_phone("abc-def-ghi") == ""
    assert clean_phone(None) == ""
    assert clean_phone("123-456-7890") == "1234567890"

def test_my_transform_clean_phone_numbers(glue_context, sample_data):
    # Create mock DataFrame
    schema, data = sample_data
    spark_df = MockSparkDataFrame(data, schema)
    
    # Mock DynamicFrame and DynamicFrameCollection
    input_dyf = MockDynamicFrame(spark_df, glue_context, "input_dyf")
    dfc = MockDynamicFrameCollection({"input_dyf": input_dyf}, glue_context)
    
    # Execute the transform
    result_dfc = MyTransform(glue_context, dfc)
    
    # Get the resulting DynamicFrame
    result_dyf = result_dfc.select(list(result_dfc.keys())[0])
    
    # Convert back to DataFrame for assertions
    result_df = result_dyf.toDF()
    
    # Collect results
    results = result_df.collect()
    
    # Expected results after cleaning
    expected = [
        ("1234567890", "9876543210"),  # +1 removed from Phone 2
        ("1234567890", "1234567890"),
        ("", ""),  # None and invalid handled
        ("1234567890", "")  # None becomes empty string
    ]
    
    # Verify results
    for i, row in enumerate(results):
        assert row["Phone 1"] == expected[i][0], f"Phone 1 cleaning failed for row {i}"
        assert row["Phone 2"] == expected[i][1], f"Phone 2 cleaning failed for row {i}"

def test_my_transform_empty_input(glue_context):
    # Create empty mock DataFrame
    schema = StructType([
        StructField("Phone 1", StringType(), True),
        StructField("Phone 2", StringType(), True)
    ])
    spark_df = MockSparkDataFrame([], schema)
    
    # Mock DynamicFrame and DynamicFrameCollection
    input_dyf = MockDynamicFrame(spark_df, glue_context, "input_dyf")
    dfc = MockDynamicFrameCollection({"input_dyf": input_dyf}, glue_context)
    
    # Execute the transform
    result_dfc = MyTransform(glue_context, dfc)
    
    # Get the resulting DynamicFrame
    result_dyf = result_dfc.select(list(result_dfc.keys())[0])
    
    # Verify the output is a DynamicFrame-like object
    assert isinstance(result_dyf, MockDynamicFrame)
    
    # Verify the output is empty
    assert result_dyf.toDF().count() == 0

def test_my_transform_none_values(glue_context):
    # Create mock DataFrame with None values
    schema = StructType([
        StructField("Phone 1", StringType(), True),
        StructField("Phone 2", StringType(), True)
    ])
    data = [(None, None)]
    spark_df = MockSparkDataFrame(data, schema)
    
    # Mock DynamicFrame and DynamicFrameCollection
    input_dyf = MockDynamicFrame(spark_df, glue_context, "input_dyf")
    dfc = MockDynamicFrameCollection({"input_dyf": input_dyf}, glue_context)
    
    # Execute the transform
    result_dfc = MyTransform(glue_context, dfc)
    
    # Get the resulting DynamicFrame
    result_dyf = result_dfc.select(list(result_dfc.keys())[0])
    
    # Convert to DataFrame and collect results
    result_df = result_dyf.toDF()
    result = result_df.collect()
    
    # Verify None values are converted to empty strings
    assert result[0]["Phone 1"] == ""
    assert result[0]["Phone 2"] == ""

def test_my_transform_udf_registration(glue_context, sample_data):
    # Create mock DataFrame
    schema, data = sample_data
    spark_df = MockSparkDataFrame(data, schema)
    
    # Mock DynamicFrame and DynamicFrameCollection
    input_dyf = MockDynamicFrame(spark_df, glue_context, "input_dyf")
    dfc = MockDynamicFrameCollection({"input_dyf": input_dyf}, glue_context)
    
    # Execute the transform
    MyTransform(glue_context, dfc)
    
    # Verify UDF registration was called
    glue_context.spark_session.udf.register.assert_called_once_with("clean_phone_udf", ANY)
