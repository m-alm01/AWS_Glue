import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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
AmazonS3_node1753696109039 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://demo-cutomers-glue/customers-100.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1753696109039")

# Script generated for node SQL Query
SqlQuery3472 = '''
SELECT YEAR(`Subscription Date`) AS Subscription_Year, COUNT(*) AS Count 
FROM customers 
GROUP BY Subscription_Year 
ORDER BY Subscription_Year;
'''
SQLQuery_node1753696131221 = sparkSqlQuery(glueContext, query = SqlQuery3472, mapping = {"customers":AmazonS3_node1753696109039}, transformation_ctx = "SQLQuery_node1753696131221")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1753696131221, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1753696079988", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1753696167278 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1753696131221, connection_type="s3", format="csv", connection_options={"path": "s3://demo-cutomers-glue/output/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1753696167278")

job.commit()