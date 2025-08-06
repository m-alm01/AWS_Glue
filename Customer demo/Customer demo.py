import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Extract the first DynamicFrame from the DynamicFrameCollection
    input_dyf = dfc.select(list(dfc.keys())[0])

    # Convert DynamicFrame to Spark DataFrame
    spark_df = input_dyf.toDF()

    # Define a Python UDF to clean phone numbers
    def clean_phone(phone):
        if phone is None:
            return ""
        return ''.join(filter(str.isdigit, str(phone)))

    # Register UDF
    spark = glueContext.spark_session
    clean_phone_udf = spark.udf.register("clean_phone_udf", clean_phone)

    # Apply UDF to clean Phone 1 and Phone 2
    cleaned_df = spark_df.withColumn("Phone 1", clean_phone_udf("Phone 1")) \
                         .withColumn("Phone 2", clean_phone_udf("Phone 2"))

    # Convert back to DynamicFrame
    cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_dyf")

    # Return as DynamicFrameCollection
    return DynamicFrameCollection({0: cleaned_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1754464284430 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://customer-data-demo1/customers-100.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1754464284430")

# Script generated for node Custom Transform
CustomTransform_node1754466550949 = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node1754464284430": AmazonS3_node1754464284430}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1754468384631 = SelectFromCollection.apply(dfc=CustomTransform_node1754466550949, key=list(CustomTransform_node1754466550949.keys())[0], transformation_ctx="SelectFromCollection_node1754468384631")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1754468384631, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754465539857", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754468803173 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1754468384631, connection_type="s3", format="csv", connection_options={"path": "s3://customer-data-demo1/output/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1754468803173")

job.commit()