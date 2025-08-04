import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, to_date
from pyspark.sql.types import *

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("GlueJobTest").getOrCreate()
    yield spark
    spark.stop()

def test_subscription_year_count(spark):
    # Load the input CSV as text to handle commas in fields
    input_rdd = spark.read.text("data/customers-100.csv").rdd.map(lambda row: row.value)
    
    # Get header and filter data
    header = input_rdd.first()
    data = input_rdd.filter(lambda line: line != header)
    
    # Parse function to handle extra commas (e.g., in Company field)
    def parse_line(line):
        parts = line.split(',')
        if len(parts) > 12:
            # Merge company field (assuming comma in 5th field: Company)
            company = parts[4] + ',' + parts[5]
            parts = parts[0:4] + [company] + parts[6:]
        return parts
    
    parsed = data.map(parse_line)
    
    # Define schema
    schema = StructType([
        StructField("Index", StringType(), True),
        StructField("Customer Id", StringType(), True),
        StructField("First Name", StringType(), True),
        StructField("Last Name", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Phone 1", StringType(), True),
        StructField("Phone 2", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Subscription Date", StringType(), True),
        StructField("Website", StringType(), True)
    ])
    
    # Create DataFrame
    input_df = spark.createDataFrame(parsed, schema)
    
    # Cast Subscription Date to date type
    input_df = input_df.withColumn("Subscription Date", to_date(col("Subscription Date"), "yyyy-MM-dd"))
    
    # Apply the SQL transformation logic from the Glue job
    transformed_df = input_df.select(
        year(col("Subscription Date")).alias("Subscription_Year")
    ).groupBy("Subscription_Year").count().withColumnRenamed("count", "Count").orderBy("Subscription_Year")
    
    # Collect results
    results = transformed_df.collect()
    
    # Assert the output matches the expected counts from the provided data
    assert len(results) == 4, "Expected 4 rows (one for each year: 2020, 2021, 2022, 2023)"
    assert results[0]["Subscription_Year"] == 2020 and results[0]["Count"] == 3, "Mismatch for 2020"
    assert results[1]["Subscription_Year"] == 2021 and results[1]["Count"] == 3, "Mismatch for 2021"
    assert results[2]["Subscription_Year"] == 2022 and results[2]["Count"] == 2, "Mismatch for 2022"
    assert results[3]["Subscription_Year"] == 2023 and results[3]["Count"] == 2, "Mismatch for 2023"
    
    # Basic data quality check (mimicking the default ruleset: ColumnCount > 0)
    assert transformed_df.count() > 0, "Data quality failed: No rows in output"
    assert len(transformed_df.columns) > 0, "Data quality failed: No columns in output"