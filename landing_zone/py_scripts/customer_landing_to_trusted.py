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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1726280715136 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1726280715136")

# Script generated for node Share Research is true
SqlQuery766 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
ShareResearchistrue_node1726256492545 = sparkSqlQuery(glueContext, query = SqlQuery766, mapping = {"myDataSource":AWSGlueDataCatalog_node1726280715136}, transformation_ctx = "ShareResearchistrue_node1726256492545")

# Script generated for node CT
CT_node1726280810320 = glueContext.getSink(path="s3://stedi-lakehouse-projec3/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CT_node1726280810320")
CT_node1726280810320.setCatalogInfo(catalogDatabase="stedi-beta",catalogTableName="customer_trusted")
CT_node1726280810320.setFormat("json")
CT_node1726280810320.writeFrame(ShareResearchistrue_node1726256492545)
job.commit()