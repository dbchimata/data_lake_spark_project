import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://customerrecords2023/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1",
)

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1684103214397 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://customerrecords2023/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_trusted_node1684103214397",
)

# Script generated for node Join
Join_node1684103258911 = Join.apply(
    frame1=Accelerometer_trusted_node1684103214397,
    frame2=step_trainer_trusted_node1,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1684103258911",
)

# Script generated for node Drop Fields
DropFields_node1684103293430 = DropFields.apply(
    frame=Join_node1684103258911,
    paths=["_right__serialNumber#0", "user"],
    transformation_ctx="DropFields_node1684103293430",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.getSink(
    path="s3://customerrecords2023/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node3",
)
machine_learning_curated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node3.setFormat("glueparquet")
machine_learning_curated_node3.writeFrame(DropFields_node1684103293430)
job.commit()