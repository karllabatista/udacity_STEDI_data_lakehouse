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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1726373146029 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="accelerometer_trusted2", transformation_ctx="AccelerometerTrusted_node1726373146029")

# Script generated for node Step Training Trusted
StepTrainingTrusted_node1726372643197 = glueContext.create_dynamic_frame.from_catalog(database="stedi-beta", table_name="step_trainer_trusted", transformation_ctx="StepTrainingTrusted_node1726372643197")

# Script generated for node SQL Query
SqlQuery1099 = '''
select *from st
join at on timestamp = sensorreadingtime;
'''
SQLQuery_node1726375470724 = sparkSqlQuery(glueContext, query = SqlQuery1099, mapping = {"at":AccelerometerTrusted_node1726373146029, "st":StepTrainingTrusted_node1726372643197}, transformation_ctx = "SQLQuery_node1726375470724")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1726373741075 = glueContext.getSink(path="s3://stedi-lakehouse-projec3/machine-learning-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1726373741075")
MachineLearningCurated_node1726373741075.setCatalogInfo(catalogDatabase="stedi-beta",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1726373741075.setFormat("json")
MachineLearningCurated_node1726373741075.writeFrame(SQLQuery_node1726375470724)
job.commit()