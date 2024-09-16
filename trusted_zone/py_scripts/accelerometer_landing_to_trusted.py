import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Trusted Customer
TrustedCustomer_node1726281129274 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="customer_trusted", transformation_ctx="TrustedCustomer_node1726281129274")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1726282257399 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1726282257399")

# Script generated for node Join
Join_node1726282881371 = Join.apply(frame1=TrustedCustomer_node1726281129274, frame2=AccelerometerLanding_node1726282257399, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1726282881371")

# Script generated for node SQL Query
SqlQuery993 = '''
select  user,timestamp,x,y,z from myDataSource

'''
SQLQuery_node1726284910817 = sparkSqlQuery(glueContext, query = SqlQuery993, mapping = {"myDataSource":Join_node1726282881371}, transformation_ctx = "SQLQuery_node1726284910817")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1726282910993 = glueContext.getSink(path="s3://stedi-lakehouse-projec3/accelerometer/trusted2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1726282910993")
AccelerometerTrusted_node1726282910993.setCatalogInfo(catalogDatabase="stedi-beta",catalogTableName="accelerometer_trusted2")
AccelerometerTrusted_node1726282910993.setFormat("json")
AccelerometerTrusted_node1726282910993.writeFrame(SQLQuery_node1726284910817)
job.commit()