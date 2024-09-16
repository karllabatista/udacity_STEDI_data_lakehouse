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

# Script generated for node Customers Trusted
CustomersTrusted_node1726285625510 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="customer_trusted", transformation_ctx="CustomersTrusted_node1726285625510")

# Script generated for node Acelerometer Trusted
AcelerometerTrusted_node1726285418973 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="accelerometer_trusted2", transformation_ctx="AcelerometerTrusted_node1726285418973")

# Script generated for node Join
Join_node1726285744571 = Join.apply(frame1=AcelerometerTrusted_node1726285418973, frame2=CustomersTrusted_node1726285625510, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1726285744571")

# Script generated for node SQL Query
SqlQuery916 = '''
select distinct customername,email,phone,serialnumber,birthday,
registrationdate,lastupdatedate,sharewithpublicasofdate,
sharewithresearchasofdate from myDataSource
'''
SQLQuery_node1726285777514 = sparkSqlQuery(glueContext, query = SqlQuery916, mapping = {"myDataSource":Join_node1726285744571}, transformation_ctx = "SQLQuery_node1726285777514")

# Script generated for node Customers Curated
CustomersCurated_node1726285937079 = glueContext.getSink(path="s3://stedi-lakehouse-projec3/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomersCurated_node1726285937079")
CustomersCurated_node1726285937079.setCatalogInfo(catalogDatabase="stedi-beta",catalogTableName="customers_curated")
CustomersCurated_node1726285937079.setFormat("json")
CustomersCurated_node1726285937079.writeFrame(SQLQuery_node1726285777514)
job.commit()