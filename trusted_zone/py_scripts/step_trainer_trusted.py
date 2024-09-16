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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1726352413281 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1726352413281")

# Script generated for node Customer Curated
CustomerCurated_node1726352226496 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="customers_curated", transformation_ctx="CustomerCurated_node1726352226496")

# Script generated for node SQL Query
SqlQuery728 = '''
SELECT *
FROM myDataSource2
WHERE serialnumber IN (SELECT serialnumber FROM myDataSource);
'''
SQLQuery_node1726354569487 = sparkSqlQuery(glueContext, query = SqlQuery728, mapping = {"myDataSource":CustomerCurated_node1726352226496, "myDataSource2":StepTrainerLanding_node1726352413281}, transformation_ctx = "SQLQuery_node1726354569487")

# Script generated for node Steo Trainer Trusted
SteoTrainerTrusted_node1726352471162 = glueContext.getSink(path="s3://stedi-lakehouse-projec3/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="SteoTrainerTrusted_node1726352471162")
SteoTrainerTrusted_node1726352471162.setCatalogInfo(catalogDatabase="stedi-beta",catalogTableName="step_trainer_trusted")
SteoTrainerTrusted_node1726352471162.setFormat("json")
SteoTrainerTrusted_node1726352471162.writeFrame(SQLQuery_node1726354569487)
job.commit()